//! This code is inspired from https://github.com/bparli/bpfs and was modified to work
//! with node-replication for benchmarking.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::str::from_utf8;
use std::time::{Duration, SystemTime};
use std::{fs, mem};

use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
// Re-export reused structs from fuse:
pub use fuser::FileAttr;
pub use fuser::FileType;

use crate::db_backend::Postgres;
use libc::{c_int, pid_t, EACCES, EEXIST, EINVAL, ENOENT, ENOTEMPTY};
use log::{debug, error, info, trace, warn};
use nix::unistd::{getsid, Pid};
use postgres::types::ToSql;
use postgres::{Client, CopyOutReader, NoTls, Row, Statement, ToStatement, Transaction};
use regex::Regex;
use reqwest::header::RANGE;
use std::io::Read;
use std::ops::Add;
use std::path::Path;

use postgres::config::SslMode;
#[cfg(feature = "ext4")]
use std::fs::{File, OpenOptions};
#[cfg(feature = "ext4")]
use std::io::{Seek, SeekFrom, Write};

mod db_backend;

pub type InodeId = u64;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Error {
    NoEntry,
    NotEmpty,
    AlreadyExists,
    ParentNotFound,
    ChildNotFound,
    NewParentNotFound,
    InvalidInput,
    AccessDenied,
}

/// Converts FS Errors to FUSE compatible libc errno types.
impl From<Error> for c_int {
    fn from(e: Error) -> c_int {
        match e {
            Error::NoEntry => ENOENT,
            Error::NotEmpty => ENOTEMPTY,
            Error::AlreadyExists => EEXIST,
            Error::InvalidInput => EINVAL,
            Error::ParentNotFound => ENOENT,
            Error::NewParentNotFound => EINVAL,
            Error::ChildNotFound => ENOENT,
            Error::AccessDenied => EACCES,
        }
    }
}

fn FileType_to_int(file_type: FileType) -> i32 {
    match file_type {
        FileType::Directory => 0,
        FileType::BlockDevice => 1,
        FileType::CharDevice => 2,
        FileType::NamedPipe => 3,
        FileType::RegularFile => 4,
        FileType::Socket => 5,
        FileType::Symlink => 6,
    }
}

fn int_to_FileType(id: i32) -> FileType {
    match id {
        0 => FileType::Directory,
        1 => FileType::BlockDevice,
        2 => FileType::CharDevice,
        3 => FileType::NamedPipe,
        4 => FileType::RegularFile,
        5 => FileType::Socket,
        _ => FileType::Symlink,
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct SetAttrRequest {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<TimeOrNow>,
    pub mtime: Option<TimeOrNow>,
    pub ctime: Option<SystemTime>,
    pub fh: Option<u64>,
    pub crtime: Option<SystemTime>,
    pub chgtime: Option<SystemTime>,
    pub bkuptime: Option<SystemTime>,
    pub flags: Option<u32>,
}

// based on https://github.com/sfackler/rust-postgres/issues/205#issuecomment-252530990
pub struct PostgresTransaction {
    trans: postgres::Transaction<'static>,
}

impl PostgresTransaction {
    fn new(conn: &mut Client) -> Result<Self, postgres::Error> {
        let trans = unsafe { mem::transmute(conn.transaction()?) };

        Ok(PostgresTransaction { trans })
    }

    fn execute<T>(
        &mut self,
        sql: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        self.trans.execute(sql, params)
    }

    fn commit(mut self) -> Result<(), postgres::Error> {
        self.trans.commit()
    }

    pub fn prepare(&mut self, sql: &str) -> Result<Statement, postgres::Error> {
        self.trans.prepare(sql)
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>, postgres::Error> {
        self.trans.transaction()
    }

    pub fn batch_execute(&mut self, sql: String) -> Result<(), postgres::Error> {
        self.trans.batch_execute(&sql)
    }

    pub fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        self.trans.query_opt(query, params)
    }

    pub fn query<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        self.trans.query(query, params)
    }

    pub fn query_one<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        self.trans.query_one(query, params)
    }

    pub fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        self.trans.copy_out(query)
    }
}

pub struct MemFilesystem {
    conn: Postgres,
    TTL: Duration,
    active_transactions: HashMap<u32, Box<PostgresTransaction>>,
    data_dir: Option<String>,
}

fn TimeOrNow_to_UnixTime(t: Option<TimeOrNow>) -> Option<SystemTime> {
    match t {
        Some(t) => match t {
            TimeOrNow::SpecificTime(specific_time) => Some(specific_time),
            TimeOrNow::Now => Some(SystemTime::now()),
        },
        None => None,
    }
}

// fn add_sysnotify_function(db: &Connection) -> Result<()> {
//     db.create_scalar_function(
//         "sysnotify",
//         2,
//         FunctionFlags::SQLITE_UTF8,
//         |ctx| -> Result<String, _> {
//             let path: String = ctx.get(1).expect("Retrieving path failed");
//             let mut pipe = OpenOptions::new().write(true).open(path).unwrap();
//             let value = ctx.get_raw(0);
//             match value {
//                 ValueRef::Null => {
//                     pipe.write("NULL".to_string().as_bytes()).unwrap();
//                 }
//                 ValueRef::Integer(value) => {
//                     pipe.write(value.to_string().as_bytes()).unwrap();
//                 }
//                 ValueRef::Real(value) => {
//                     pipe.write(value.to_string().as_bytes()).unwrap();
//                 }
//                 ValueRef::Text(value) => {
//                     pipe.write(value).unwrap();
//                 }
//                 ValueRef::Blob(_) => {}
//             }
//             Ok("".to_string())
//         },
//     )
// }

impl MemFilesystem {
    pub fn new() -> MemFilesystem {
        MemFilesystem::new_path("fs")
    }

    pub fn set_data_dir(&mut self, path: &str) {
        self.data_dir = Some(path.to_string());
    }

    pub fn new_path(path: &str) -> MemFilesystem {
        let mut config = Client::configure();
        config
            .host("localhost")
            .user("postgres")
            .password("postgres")
            .dbname(path);
        let mut conn = Postgres {
            conn: Some(config.connect(NoTls).expect("Cannot open connection to DB")),
        };

        let mut tx = conn.transaction().unwrap();

        // Secure type creation based on https://stackoverflow.com/a/48382296
        tx.batch_execute(
            "CREATE TABLE if not exists inodes (
            inode BIGINT PRIMARY KEY CHECK(inode > 0),
            data bytea,
            oid integer,
            size BIGINT NOT NULL DEFAULT 0,
            blocks INTEGER NOT NULL DEFAULT 0 CHECK(blocks >= 0),
            atime TIMESTAMP NOT NULL,
            mtime TIMESTAMP NOT NULL,
            ctime TIMESTAMP NOT NULL,
            crtime TIMESTAMP NOT NULL,
            kind INTEGER NOT NULL,
            perm SMALLINT NOT NULL DEFAULT 511,
            nlink INTEGER NOT NULL DEFAULT 1 CHECK(nlink >= 0),
            uid INTEGER NOT NULL DEFAULT 0,
            gid INTEGER NOT NULL DEFAULT 0,
            rdev INTEGER NOT NULL DEFAULT 0,
            flags INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE if not exists tree (
            inode BIGINT,
            parent BIGINT,
            name TEXT NOT NULL,
            UNIQUE(parent, name),
            FOREIGN KEY(inode) REFERENCES inodes(inode),
            FOREIGN KEY(parent) REFERENCES inodes(inode)
            );
            CREATE INDEX IF NOT EXISTS tree_inode_index ON tree (inode);
            CREATE INDEX IF NOT EXISTS tree_name_index ON tree (name);
            CREATE TABLE if not exists dependencies (
            inode BIGINT,
            dep_inode BIGINT NOT NULL,
            type TEXT NOT NULL,
            FOREIGN KEY(inode) REFERENCES inodes(inode) ON DELETE CASCADE
            );
            DO $$ BEGIN
                CREATE TYPE event_type AS ENUM ('write', 'read');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            CREATE TABLE if not exists audit (
            inode BIGINT,
            uid INTEGER NOT NULL,
            event event_type NOT NULL,
            change_begin BIGINT CHECK(change_begin >= 0),
            change_end BIGINT CHECK(change_end >= change_begin),
            date TIMESTAMP NOT NULL
            );
            CREATE TABLE if not exists access (
            inode BIGINT,
            bw BOOLEAN NOT NULL,
            prog TEXT NOT NULL,
            FOREIGN KEY(inode) REFERENCES inodes(inode) ON DELETE CASCADE
            );
            CREATE TABLE if not exists xattr (
            inode BIGINT,
            key Text NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY(inode) REFERENCES inodes(inode) ON DELETE CASCADE
            );
            CREATE OR REPLACE FUNCTION cleanup() RETURNS trigger
            AS $$
            BEGIN
            If NEW.oid <> NULL then
                SELECT lo_unlink(NEW.oid);
            end if;
            DELETE FROM inodes WHERE inode = NEW.inode;
            RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            DROP TRIGGER IF EXISTS cleanup_trigger ON inodes;
            CREATE TRIGGER cleanup_trigger
            AFTER UPDATE OF nlink ON inodes
            FOR EACH ROW
            WHEN (NEW.nlink = 0)
            EXECUTE PROCEDURE cleanup();
            CREATE OR REPLACE FUNCTION get_path(text) RETURNS text
            AS $$
            WITH RECURSIVE paths as (
                SELECT parent, name
                FROM tree
                WHERE name = $1
                UNION ALL
                SELECT tree.parent, tree.name || '/' || paths.name
                FROM tree, paths
                WHERE paths.parent = tree.inode AND tree.inode <> 1)
            SELECT name FROM paths WHERE parent = 1 LIMIT 1
            $$ LANGUAGE SQL;
        ",
        )
        .unwrap();
        if cfg!(feature = "lob") {
            tx.batch_execute("
            CREATE OR REPLACE FUNCTION write_slice(bigint, int, bigint, bigint, timestamp, bytea) RETURNS void
            AS $$
                INSERT INTO audit (inode, uid, event, change_begin, change_end, date)
                VALUES ($1, $2, 'write', $3, $4, $5);
                UPDATE inodes as i
                SET size=GREATEST(size, $4), atime = $5, mtime = $5 \
                WHERE i.inode = $1 \
                RETURNING lo_put(oid, $3, $6)
            $$ LANGUAGE SQL;
        "
        )
        .unwrap();
        }

        tx.execute(
            "INSERT INTO inodes (inode, atime, mtime, ctime, crtime, kind) \
            VALUES (1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $1) \
            ON CONFLICT DO NOTHING",
            &[&FileType_to_int(FileType::Directory)],
        )
        .unwrap();

        tx.execute(
            "INSERT INTO tree VALUES (1, 1, '/') ON CONFLICT DO NOTHING",
            &[],
        )
        .unwrap();

        tx.commit().unwrap();

        MemFilesystem {
            conn,
            TTL: Duration::new(1, 0),
            active_transactions: HashMap::new(),
            data_dir: None,
        }
    }

    pub fn getattr(&mut self, ino: u64, pid: u32) -> Result<FileAttr, Error> {
        debug!("getattr(ino={})", ino);
        let ino_sigend = ino as i64;
        let row =  self.conn.query_opt("\
        SELECT inode, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, flags \
        FROM inodes WHERE inode = $1", &[&ino_sigend]).unwrap();
        match row {
            Some(row) => {
                let mut fa = FileAttr {
                    ino: row.get::<_, i64>(0) as u64,
                    size: row.get::<_, i64>(1) as u64,
                    blocks: row.get::<_, i32>(2) as u64,
                    atime: row.get(3),
                    mtime: row.get(4),
                    ctime: row.get(5),
                    crtime: row.get(6),
                    kind: int_to_FileType(row.get(7)),
                    perm: row.get::<_, i16>(8) as u16,
                    nlink: row.get::<_, i32>(9) as u32,
                    uid: row.get::<_, i32>(10) as u32,
                    gid: row.get::<_, i32>(11) as u32,
                    rdev: row.get::<_, i32>(12) as u32,
                    blksize: 0,
                    flags: row.get::<_, i32>(13) as u32,
                };
                if fa.flags == 1 {
                    fa.size = self.read(ino, 0, 0, 0, 0, pid).unwrap().len() as u64;
                }
                Ok(fa)
            }
            None => Err(Error::NoEntry),
        }
    }

    pub fn getxattr(&mut self, ino: u64, name: &OsStr) -> Result<String, Error> {
        let name = name.to_str().unwrap();
        let ino_signed = ino as i64;
        debug!("getxattr(ino={}, name={})", ino, name);

        if name.starts_with("audit") {
            let result: Vec<String> = self
                .conn
                .query("
                WITH timestamps as (
                    SELECT DISTINCT change_begin as stamp FROM audit
                    UNION
                    SELECT DISTINCT change_end as stamp FROM audit
                )
                SELECT uid || ' : ' || change_begin || ' : ' || date
                FROM (
                    SELECT DISTINCT  *, Lag(uid) OVER (ORDER By stamp)
                    FROM (
                        SELECT DISTINCT ON (stamp) *
                        FROM timestamps, audit
                        WHERE inode = $1 AND stamp BETWEEN change_begin AND change_end AND stamp <= (SELECT size FROM inodes WHERE inode = $1)
                        ORDER BY stamp, date) as i
                    ORDER BY stamp) as t
                WHERE uid <> lag or lag is null",
                    &[&ino_signed],
                )
                .unwrap_or_default()
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect();
            return Ok(result.join(","));
        }

        if name.starts_with("depends#") {
            let name = name.strip_prefix("depends#");
            let result: Vec<String> = self
                .conn
                .query(
                    "SELECT name FROM tree, dependencies \
                    WHERE type = $1 AND dependencies.inode = $2 AND tree.inode = dependencies.dep_inode",
                    &[&name, &ino_signed],
                )
                .unwrap_or_default()
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect();
            return Ok(result.join(","));
        }

        if name.starts_with("isdepof#") {
            let name = name.strip_prefix("isdepof#");
            let result: Vec<String> = self
                .conn
                .query(
                    "SELECT name FROM tree, dependencies \
                    WHERE type = $1 AND dependencies.dep_inode = $2 AND tree.inode = dependencies.inode",
                    &[&name, &ino_signed],
                )
                .unwrap_or_default()
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect();
            return Ok(result.join(","));
        }

        if name.starts_with("sql#") {
            let mut tx = self.conn.transaction().unwrap();
            let name = name.strip_prefix("sql#").unwrap();
            let mut str_buf: Vec<u8> = Vec::new();
            if name.to_lowercase().starts_with("select")
                || name.to_lowercase().contains("returning")
            {
                match tx.copy_out(&*format!("COPY ({}) TO STDOUT (FORMAT csv)", name)) {
                    Ok(mut result) => {
                        match result.read_to_end(&mut str_buf) {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{}", e);
                                return Err(Error::InvalidInput);
                            }
                        };
                    }
                    Err(e) => {
                        error!("{}", e);
                        return Err(Error::InvalidInput);
                    }
                };
                tx.commit().unwrap();
                return Ok(String::from_utf8(str_buf).unwrap());
            } else {
                return match tx.execute(name, &[]) {
                    Ok(_) => Ok(String::new()),
                    Err(_) => Err(Error::InvalidInput),
                };
            }
        };

        if name.starts_with("startTransaction") {
            let sid: u32 = name
                .strip_prefix("startTransaction")
                .unwrap()
                .parse()
                .unwrap();
            self.active_transactions.insert(
                sid,
                Box::new(PostgresTransaction::new(self.conn.conn.as_mut().unwrap()).unwrap()),
            );
            return Ok("".to_string());
        }

        if name.starts_with("endTransaction") {
            let sid: u32 = name
                .strip_prefix("endTransaction")
                .unwrap()
                .parse()
                .unwrap();
            let t = self.active_transactions.remove(&sid);
            match t {
                Some(t) => {
                    t.trans.commit().unwrap();
                    return Ok("".to_string());
                }
                None => {
                    return Ok("".to_string());
                }
            }
        }

        if name.starts_with("block#") {
            let name = name.strip_prefix("block#").unwrap();
            let mut parts = name.split('#');
            let prog = parts.next().unwrap();
            self.conn
                .execute(
                    "\
            INSERT INTO access VALUES ($1, false, $2) ON CONFLICT DO NOTHING",
                    &[&ino_signed, &prog],
                )
                .unwrap();
            return Ok(String::new());
        }

        if name.starts_with("rmblock#") {
            let name = name.strip_prefix("rmblock#").unwrap();
            let mut parts = name.split('#');
            let prog = parts.next().unwrap();
            self.conn
                .execute(
                    "\
            DELETE FROM access WHERE inode = $1 AND prog = $2",
                    &[&ino_signed, &prog],
                )
                .unwrap();
            return Ok(String::new());
        }

        // Normal xattr
        let result: Vec<String> = self
            .conn
            .query_opt(
                "SELECT value FROM xattr \
                    WHERE xattr.inode = $2 AND xattr.name = $1",
                &[&name, &ino_signed],
            )
            .unwrap_or_default()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        Ok(result.join(","))
    }

    /// Lists all extended attributes of a file.
    pub fn listxattr(&mut self, ino: u64) -> Result<Vec<u8>, Error> {
        let ino_signed = ino as i64;
        debug!("listxattr(ino={})", ino);

        let result: Vec<String> = self
            .conn
            .query(
                "SELECT DISTINCT type FROM dependencies \
            WHERE inode = $1",
                &[&ino_signed],
            )
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        Ok(result.join(",").into_bytes())
    }

    fn removexattr(&mut self, ino: u64, name: &OsStr) -> Result<(), Error> {
        let ino_signed = ino as i64;
        let name_str = name.to_str().unwrap();

        if name_str.starts_with("depends#") {
            let name_str = name_str.strip_prefix("depends#").unwrap();
            let mut parts = name_str.split('#');

            let type_str = parts.next().unwrap();
            let name_str = parts.next().unwrap();
            self.conn
                .execute(
            //         \ WITH RECURSIVE i as (
            // SELECT inode as p,  right($2, length($2) - strpos($2, '/')) as remainder FROM tree WHERE parent = 1 AND name = split_part($2, '/',1)
            // UNION
            // SELECT inode as p,  right(remainder, length(remainder) - strpos(remainder, '/')) as remainder FROM i, tree WHERE tree.parent = i.p AND name = split_part(remainder, '/',1) AND remainer <> ''
            // )
            "DELETE FROM dependencies WHERE inode = $1 AND dep_inode = (SELECT inode FROM tree WHERE name = $2 LIMIT 1) AND type = $3;",
                    &[&ino_signed, &name_str, &type_str],
                )
                .unwrap();
            return Ok(());
        }

        self.conn
            .execute(
                "\
            DELETE FROM xattr WHERE inode = $1 AND key = $2",
                &[&ino_signed, &name_str],
            )
            .unwrap();
        Ok(())
    }

    /// Updates the attributes on an inode with values in `new_attrs`.
    pub fn setattr(
        &mut self,
        ino: u64,
        new_attrs: SetAttrRequest,
        pid: u32,
    ) -> Result<FileAttr, Error> {
        debug!("setattr(ino={}, new_attrs={:?})", ino, new_attrs);

        let size = new_attrs.size.map(|s| s as i64);
        let mode = new_attrs.mode.map(|s| s as i16);
        let uid = new_attrs.uid.map(|s| s as i32);
        let gid = new_attrs.gid.map(|s| s as i32);
        let atime = TimeOrNow_to_UnixTime(new_attrs.atime);
        let mtime = TimeOrNow_to_UnixTime(new_attrs.mtime);

        if let Some(s) = size {
            self.conn.execute(
                "SELECT pg_catalog.lo_truncate64(lo_open(oid, CAST(x'20000' | x'40000' AS integer)), $2::BIGINT) \
                FROM inodes WHERE inode = $1",
                &[&(ino as i64), &s]).unwrap();
        }
        let row = self
            .conn
            .query_opt(
                "\
        UPDATE inodes \
        SET \
            -- data= CASE
            --     WHEN $2::BIGINT IS NULL THEN data
            --     ELSE SUBSTRING(data FROM 1 FOR ($2 + 1)::INTEGER)
            -- END,
            size=COALESCE($2, size), \
            perm=COALESCE($3, perm), \
            uid=COALESCE($4, uid), \
            gid=COALESCE($5, gid), \
            atime=COALESCE($6, atime), \
            mtime=COALESCE($7, mtime), \
            crtime=COALESCE($8, crtime) \
        WHERE inode = $1 \
        RETURNING \
            size, \
            blocks, \
            atime, \
            mtime, \
            ctime, \
            crtime, \
            kind, \
            perm, \
            nlink, \
            uid, \
            gid, \
            rdev, \
            flags",
                &[
                    &(ino as i64),
                    &size,
                    &mode,
                    &uid,
                    &gid,
                    &atime,
                    &mtime,
                    &new_attrs.crtime,
                ],
            )
            .unwrap();
        match row {
            Some(row) => {
                let mut fa = FileAttr {
                    ino,
                    size: row.get::<_, i64>(0) as u64,
                    blocks: row.get::<_, i32>(1) as u64,
                    atime: row.get(2),
                    mtime: row.get(3),
                    ctime: row.get(4),
                    crtime: row.get(5),
                    kind: int_to_FileType(row.get(6)),
                    perm: row.get::<_, i16>(7) as u16,
                    nlink: row.get::<_, i32>(8) as u32,
                    uid: row.get::<_, i32>(9) as u32,
                    gid: row.get::<_, i32>(10) as u32,
                    rdev: row.get::<_, i32>(11) as u32,
                    blksize: 0,
                    flags: row.get::<_, i32>(12) as u32,
                };
                if fa.flags == 1 {
                    fa.size = self.read(ino, 0, 0, 0, 0, pid).unwrap().len() as u64;
                }
                Ok(fa)
            }
            None => Err(Error::NoEntry),
        }
    }

    /// Updates the extended attributes on an file.
    pub fn setxattr(&mut self, ino: u64, name: &OsStr, value: &[u8]) -> Result<(), Error> {
        let ino_signed = ino as i64;
        let name = name.to_str().unwrap();
        let value = from_utf8(value).unwrap();
        debug!("setxattr(ino={}, name={}, value={})", ino, name, value);

        let changed_rows;
        if name.starts_with("depends#") {
            let name = name.strip_prefix("depends#");
            changed_rows = self
                .conn
                .execute(
                    "\
         INSERT INTO dependencies (inode, dep_inode, type)  \
         SELECT $1, tree.inode, $3 FROM tree \
         WHERE EXISTS (SELECT * FROM inodes WHERE inode = $1) AND tree.name = $2",
                    &[&ino_signed, &value, &name],
                )
                .unwrap();
        } else {
            changed_rows = self
                .conn
                .execute(
                    "\
         INSERT INTO xattr (inode, key, value)  \
         SELECT $1, $3, $2 FROM inodes WHERE inode = $1)",
                    &[&ino_signed, &value, &name],
                )
                .unwrap();
        }
        if changed_rows != 0 {
            Ok(())
        } else {
            Err(Error::NoEntry)
        }
    }

    pub fn readdir(
        &mut self,
        ino: InodeId,
        _fh: u64,
    ) -> Result<Vec<(InodeId, FileType, String)>, Error> {
        let ino_signed = ino as i64;
        debug!("readdir(ino={}, fh={})", ino, _fh);
        let mut entries: Vec<(u64, FileType, String)> = Vec::with_capacity(32);
        entries.push((ino, FileType::Directory, String::from(".")));

        entries.extend(
            self.conn
                .query(
                    "SELECT parent, $2 as kind, '..' as name FROM tree WHERE inode = $1 \
            UNION ALL \
            SELECT inodes.inode, kind, name \
            FROM inodes, tree \
            WHERE inodes.inode = tree.inode and tree.parent = $1 and name <> '/'",
                    &[&ino_signed, &FileType_to_int(FileType::Directory)],
                )
                .unwrap()
                .iter()
                .map(|row| {
                    (
                        row.get::<_, i64>(0) as u64,
                        int_to_FileType(row.get::<_, i32>(1)),
                        row.get::<_, String>(2),
                    )
                }),
        );

        debug!("Results: {:?}", entries);

        Ok(entries)
    }

    pub fn lookup(&mut self, parent: u64, name: &OsStr) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("lookup(parent={}, name={})", parent, name_str);

        if name_str.starts_with("http") {
            let client = reqwest::blocking::Client::new();
            let link = name_str.replace("\\", "/");
            let link = link.replace("//", "://");
            debug!("File size: {}", link);
            let request = match client.head(link.clone()).build() {
                Ok(request) => request,
                Err(e) => {
                    println!("{}: {}", e.to_string(), link);
                    return Err(Error::InvalidInput);
                }
            };
            let response = client.execute(request).unwrap();
            let size = response
                .headers()
                .get("Content-Length")
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let ts_now = SystemTime::now();
            let ino = self
                .conn
                .query_one(
                    "UPDATE inodes SET size = $2 WHERE data = $1 RETURNING inode",
                    &[&name_str.as_bytes(), &(size as i64)],
                )
                .unwrap()
                .get::<_, i64>(0) as u64;
            debug!("File size: {}", size);
            return Ok(FileAttr {
                ino,
                size,
                blocks: 0,
                atime: ts_now,
                mtime: ts_now,
                ctime: ts_now,
                crtime: ts_now,
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            });
        }

        match self.conn.query_opt("SELECT inodes.inode, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, flags \
            FROM inodes, tree \
            WHERE inodes.inode = tree.inode AND parent = $1 AND name = $2", &[&(parent as i64), &name_str]).unwrap() {
            Some(row) => {
                Ok(FileAttr {
                    ino: row.get::<_, i64>(0) as u64,
                    size: row.get::<_, i64>(1) as u64,
                    blocks: row.get::<_, i32>(2) as u64,
                    atime: row.get(3),
                    mtime: row.get(4),
                    ctime: row.get(5),
                    crtime: row.get(6),
                    kind: int_to_FileType(row.get(7)),
                    perm: row.get::<_, i16>(8) as u16,
                    nlink: row.get::<_, i32>(9) as u32,
                    uid: row.get::<_, i32>(10) as u32,
                    gid: row.get::<_, i32>(11) as u32,
                    rdev: row.get::<_, i32>(12) as u32,
                    blksize: 0,
                    flags: row.get::<_, i32>(13) as u32,
                })
            }
            None => Err(Error::NoEntry),
        }
    }

    pub fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let parent_signed = parent as i64;
        let name_str = name.to_str().unwrap();
        debug!("rmdir(parent={}, name={})", parent, name_str);

        let mut tx = self.conn.transaction().unwrap();

        let result = tx
            .query_one(
                "\
            SELECT count(*) FROM tree \
            WHERE parent = (SELECT inode FROM tree WHERE parent = $1 AND name = $2)",
                &[&parent_signed, &name_str],
            )
            .and_then(|row| Ok(row.get::<_, i64>(0) as u64));
        if result.is_err() {
            return Err(Error::NoEntry);
        }
        let number_of_children = result.unwrap();
        if number_of_children > 0 {
            return Err(Error::NotEmpty);
        }
        let changed_rows = tx
            .execute(
                "\
        WITH deleted AS (DELETE FROM tree WHERE parent = $1 AND name = $2 RETURNING inode)
        UPDATE inodes as i SET nlink = nlink - 1 \
        FROM deleted as d
        WHERE i.inode = d.inode",
                &[&parent_signed, &name_str],
            )
            .unwrap();

        if changed_rows == 0 {
            tx.rollback();
            return Err(Error::NoEntry);
        }

        match tx.commit() {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::InvalidInput),
        }
    }

    pub fn mkdir(&mut self, parent: u64, name: &OsStr, mode: u32) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("mkdir(parent={}, name={})", parent, name_str);

        self.create_entry(parent, 0, name_str, mode, FileType::Directory)
    }

    pub fn unlink(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let parent_signed = parent as i64;

        let name_str = name.to_str().unwrap();
        debug!("unlink(parent={}, name={})", parent, name_str);

        let mut tx = self.conn.transaction().unwrap();
        let changed_rows = tx
            .execute(
                "\
        WITH deleted AS (DELETE FROM tree WHERE parent = $1 AND name = $2 RETURNING inode)
        UPDATE inodes as i
        SET nlink = nlink - 1 \
        FROM deleted as d
        WHERE i.inode = d.inode;",
                &[&parent_signed, &name_str],
            )
            .unwrap();
        if changed_rows == 0 {
            tx.rollback().unwrap();
            return Err(Error::NoEntry);
        }
        match tx.commit() {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::InvalidInput),
        }
    }

    pub fn create(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!(
            "create(parent={}, name={}, mode={}, flags={})",
            parent, name_str, mode, flags,
        );

        self.create_entry(parent, flags, name_str, mode, FileType::RegularFile)
    }

    fn create_entry(
        &mut self,
        parent: u64,
        flags: i32,
        name_str: &str,
        mode: u32,
        file_type: FileType,
    ) -> Result<FileAttr, Error> {
        let new_inode_nr = self
            .conn
            .query_one("SELECT max(inode) as max_index FROM inodes", &[])
            .unwrap()
            .get::<_, i64>(0)
            + 1;
        let parent_signed = parent as i64;

        let mut tx = self.conn.transaction().unwrap();
        let mut now = SystemTime::now();
        let time = tx.query_opt(
            "\
        INSERT INTO inodes (inode, atime, mtime, ctime, crtime, kind, flags, perm) \
        SELECT $3, $2, $2, $2, $2, $1, $6, $7
        WHERE EXISTS (SELECT * FROM inodes WHERE inodes.inode = $4 AND inodes.kind = $5)
        RETURNING atime",
            &[
                &FileType_to_int(file_type),
                &now,
                &new_inode_nr,
                if flags == 10 { &1 } else { &parent_signed },
                &FileType_to_int(FileType::Directory),
                &flags,
                &(mode as i16),
            ],
        );
        match time.unwrap() {
            Some(t) => {
                // Update the insert timestamp to incorporate rounding inside the DB
                now = t.get(0);
            }
            None => {
                tx.rollback().unwrap();
                return Err(Error::ParentNotFound);
            }
        }

        if cfg!(feature = "lob") {
            tx.execute(
                "UPDATE inodes SET oid = lo_creat(-1) WHERE inode = $1",
                &[&new_inode_nr],
            )
            .unwrap();
        }

        let err = tx.execute(
            "\
        INSERT INTO tree (inode, parent, name)\
        SELECT $1, $2, $3 \
        WHERE NOT EXISTS (SELECT * FROM tree WHERE tree.name = $3 AND parent = $2)",
            &[&new_inode_nr, &parent_signed, &name_str],
        );
        if err.unwrap() == 0 {
            tx.rollback().unwrap();
            return Err(Error::AlreadyExists);
        }
        match tx.commit() {
            Ok(_) => Ok(FileAttr {
                ino: new_inode_nr as u64,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: file_type,
                perm: mode as u16,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: flags as u32,
            }),
            Err(_) => Err(Error::InvalidInput),
        }
    }

    pub fn write(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        uid: u32,
        pid: u32,
        _flags: i32,
    ) -> Result<u64, Error> {
        let ino_signed = ino as i64;
        debug!("write(ino={}, fh={}, offset={})", ino, fh, offset);

        let mut exe = String::new();
        if pid != 0 {
            match fs::read_link(format!("/proc/{}/exe", pid)) {
                Ok(p) => {
                    exe = p.to_str().unwrap().to_string();
                }
                Err(_) => {
                    exe = String::new();
                }
            }
        }

        let query_result = self
            .conn
            .query_opt(
                "SELECT parent, name, t.perm FROM tree, (SELECT CASE \
                    WHEN EXISTS (SELECT * FROM access WHERE prog = $2 AND inode = $1) \
                THEN 1 ELSE 0 END as perm) as t WHERE inode = $1",
                &[&ino_signed, &exe],
            )
            .unwrap();
        let parent;
        let file_name;
        let permission;
        match query_result {
            Some(row) => {
                parent = row.get::<_, i64>(0) as u64;
                file_name = row.get::<_, String>(1);
                permission = row.get::<_, i32>(2);
            }
            None => return Err(Error::NoEntry),
        }
        if permission == 1 {
            warn!("Access denied: {} {}", exe, ino_signed);
            return Err(Error::AccessDenied);
        }

        // Structured files
        if file_name.ends_with(".sql") {
            let re = Regex::new(r"CREATE TABLE ([[:alpha:]]+)").unwrap();
            let data_str = String::from_utf8_lossy(data);
            for cap in re.captures_iter(data_str.as_ref()) {
                let f = self.create(parent, (&cap[1]).as_ref(), 0, 1);
                let ino_str = format!("{}", ino);
                self.write(f.unwrap().ino, 0, 0, ino_str.as_bytes(), uid, pid, 1)
                    .unwrap();
                println!("{}", &cap[1]);
            }
            match self.conn.batch_execute(data_str.to_string()) {
                Ok(_) => {}
                Err(e) => {
                    println!("{}", e);
                    return Err(Error::InvalidInput);
                }
            };
            // tx.execute(
            //     "UPDATE inodes SET size=$2 WHERE inode = $4",
            //     &[length, &ino_signed],
            // )
            //     .unwrap();
        }

        let sid = getsid(Some(Pid::from_raw(pid as pid_t))).unwrap().as_raw();

        match self.active_transactions.get_mut(&(sid as u32)) {
            None => {
                let mut tx =
                    Box::new(PostgresTransaction::new(self.conn.conn.as_mut().unwrap()).unwrap());
                match MemFilesystem::write_impl(
                    ino,
                    offset,
                    data,
                    uid,
                    pid,
                    &mut tx,
                    ino_signed,
                    self.data_dir.clone(),
                ) {
                    Ok(r) => match tx.commit() {
                        Ok(_) => Ok(r),
                        Err(_) => Err(Error::InvalidInput),
                    },
                    Err(e) => Err(e),
                }
            }
            Some(tx) => MemFilesystem::write_impl(
                ino,
                offset,
                data,
                uid,
                pid,
                tx,
                ino_signed,
                self.data_dir.clone(),
            ),
        }
    }

    #[cfg(feature = "lob")]
    fn write_impl(
        ino: u64,
        offset: i64,
        data: &[u8],
        uid: u32,
        pid: u32,
        tx: &mut Box<PostgresTransaction>,
        ino_signed: i64,
        data_dir: Option<String>,
    ) -> Result<u64, Error> {
        let now = SystemTime::now();
        // tx.execute("\
        //     WITH audit_temp AS (
        //         INSERT INTO audit (inode, uid, event, change_begin, change_end, date)
        //         VALUES ($1, $2, 'write', $3, $4, $5)
        //         RETURNING inode)
        //     UPDATE inodes as i
        //     SET size=GREATEST(size, $4), atime = $5, mtime = $5 \
        //     FROM audit_temp
        //     WHERE i.inode = audit_temp.inode \
        //     RETURNING lo_put(oid, $3, $6)",
        //     &[&ino_signed, &(uid as i32), &offset, &(offset + data.len() as i64), &now, &data],
        // )
        //     .unwrap();

        tx.execute(
            "\
            SELECT write_slice($1, $2, $3, $4, $5, $6)",
            &[
                &ino_signed,
                &(uid as i32),
                &offset,
                &(offset + data.len() as i64),
                &now,
                &data,
            ],
        )
        .unwrap();

        trace!(
            "write done(ino={}, wrote={}, offset={})",
            ino,
            data.len(),
            offset,
        );

        Ok(data.len() as u64)
    }

    #[cfg(feature = "bytea")]
    fn write_impl(
        ino: u64,
        offset: i64,
        data: &[u8],
        uid: u32,
        pid: u32,
        tx: &mut Box<PostgresTransaction>,
        ino_signed: i64,
        dataDir: Option<String>,
    ) -> Result<u64, Error> {
        let now = SystemTime::now();

        // BLOB variant
        tx.execute(
            "UPDATE inodes SET \
            data=overlay(COALESCE(data,'') || DECODE(REPEAT('00', GREATEST(0, $2 - octet_length(data))), 'hex') PLACING $1 FROM $2), \
            size=GREATEST(octet_length(data), $2 + octet_length($1) - 1), atime = $3, mtime = $3 WHERE inode = $4",
            &[&data, &((offset+1) as i32), &now, &ino_signed],
        ).unwrap();

        tx.execute(
            "INSERT INTO audit (inode, uid, change_begin, change_end, date) VALUES ($1, $2, $3, $4, $5)",
            &[&ino_signed, &(uid as i32), &offset, &(offset + data.len() as i64), &now],
        ).unwrap();

        trace!(
            "write done(ino={}, wrote={}, offset={})",
            ino,
            data.len(),
            offset
        );

        Ok(data.len() as u64)
    }

    #[cfg(feature = "ext4")]
    fn write_impl(
        ino: u64,
        offset: i64,
        data: &[u8],
        uid: u32,
        pid: u32,
        tx: &mut Box<PostgresTransaction>,
        ino_signed: i64,
        dataDir: Option<String>,
    ) -> Result<u64, Error> {
        // Regular Files
        let path = Path::new(dataDir.as_ref().unwrap()).join(ino.to_string());
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        f.write_all_at(data, offset as u64).unwrap();

        let now = SystemTime::now();

        let stmt = tx
            .prepare(
                "\
        WITH audit_temp AS (
            INSERT INTO audit (inode, uid, event, change_begin, change_end, date)
            VALUES ($3, $4, 'write', $5, $6, $2)
            RETURNING inode)
        UPDATE inodes AS i \
        SET size=$1, atime = $2, mtime = $2 \
        FROM audit_temp
        WHERE i.inode = audit_temp.inode",
            )
            .unwrap();
        tx.execute(
            &stmt,
            &[
                &(data.len() as i64),
                &now,
                &ino_signed,
                &(uid as i32),
                &offset,
                &(offset + data.len() as i64),
            ],
        )
        .unwrap();

        // tx.execute(
        //     "UPDATE inodes SET size=$1, atime = $2, mtime = $2 WHERE inode = $3",
        //     &[&(data.len() as i64), &now, &ino_signed],
        // ).unwrap();
        //
        // tx.execute(
        //     "INSERT INTO audit (inode, uid, change_begin, change_end, date) VALUES ($1, $2, $3, $4, $5)",
        //     &[&ino_signed, &(uid as i32), &offset, &(offset + data.len() as i64), &now],
        // ).unwrap();

        trace!(
            "write done(ino={}, wrote={}, offset={})",
            ino,
            data.len(),
            offset
        );

        Ok(data.len() as u64)
    }

    pub fn read(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        uid: u32,
        pid: u32,
    ) -> Result<Vec<u8>, Error> {
        let ino_signed = ino as i64;
        debug!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino, fh, offset, size
        );

        self.check_access(&pid, &ino_signed)?;

        let sid = getsid(Some(Pid::from_raw(pid as pid_t))).unwrap().as_raw();

        match self.active_transactions.get_mut(&(sid as u32)) {
            None => {
                let mut tx =
                    Box::new(PostgresTransaction::new(self.conn.conn.as_mut().unwrap()).unwrap());
                match MemFilesystem::read_impl(
                    ino,
                    offset,
                    size,
                    uid,
                    &mut tx,
                    self.data_dir.clone(),
                ) {
                    Ok(r) => match tx.commit() {
                        Ok(_) => Ok(r),
                        Err(_) => Err(Error::InvalidInput),
                    },
                    Err(e) => Err(e),
                }
            }
            Some(tx) => MemFilesystem::read_impl(ino, offset, size, uid, tx, self.data_dir.clone()),
        }
    }

    fn check_access(&mut self, pid: &u32, ino_signed: &i64) -> Result<(), Error> {
        // Old version with sysinfo crate which was too slow
        // let mut sys = System::new();
        // sys.refresh_processes();
        // let calling_proc = match sys.processes().get(&(*pid as pid_t)) {
        //     None => return Ok(()),
        //     Some(p) => p,
        // };

        if *pid == 0 {
            return Ok(());
        }

        let exe;
        match fs::read_link(format!("/proc/{}/exe", pid)) {
            Ok(p) => {
                exe = p.to_str().unwrap().to_string();
            }
            Err(_) => return Ok(()),
        }

        if self
            .conn
            .query_one(
                "SELECT CASE \
                    WHEN EXISTS (SELECT * FROM access WHERE prog = $1 AND inode = $2) \
                THEN 1 ELSE 0 END",
                &[&exe, &ino_signed],
            )
            .unwrap()
            .get::<_, i32>(0)
            > 0
        {
            warn!("Access denied: {} {}", exe, ino_signed);
            Err(Error::AccessDenied)
        } else {
            warn!("Access granted: {} {}", exe, ino_signed);
            Ok(())
        }
    }

    fn read_impl(
        ino: u64,
        offset: i64,
        size: u32,
        uid: u32,
        tx: &mut Box<PostgresTransaction>,
        data_dir: Option<String>,
    ) -> Result<Vec<u8>, Error> {
        let ino_signed = ino as i64;
        let query_result = tx
            .query_opt(
                "\
        SELECT flags, name, kind FROM tree, inodes \
        WHERE tree.inode = inodes.inode AND tree.inode = $1",
                &[&ino_signed],
            )
            .unwrap()
            .map(|row| {
                (
                    row.get::<_, i32>(0) as u32,
                    row.get::<_, String>(1),
                    int_to_FileType(row.get::<_, i32>(2)),
                )
            });
        let flags;
        let file_name;
        let file_type;
        match query_result {
            Some((f, n, t)) => {
                flags = f;
                file_name = n;
                file_type = t;
            }
            None => return Err(Error::NoEntry),
        }

        debug!("{}", file_name);

        if file_type == FileType::Symlink {
            let target: Vec<u8> = tx
                .query_one("SELECT data FROM inodes WHERE inode = $1", &[&ino_signed])
                .unwrap()
                .get(0);
            let target = String::from_utf8(target).unwrap();
            let target = target.replace("\\", "/").replace("//", "://");
            trace!("URL: {}", target);
            if target.starts_with("http") {
                let client = reqwest::blocking::Client::new();
                let request = client
                    .get(target)
                    .header(
                        RANGE,
                        format!("bytes={}-{}", offset, offset + size as i64 - 1),
                    )
                    .build()
                    .unwrap();
                let response = client.execute(request).unwrap();
                let res = response.bytes().unwrap().to_vec();
                println!("res: {}", String::from_utf8(res.clone()).unwrap());
                return Ok(res);
            } else {
                return Err(Error::InvalidInput);
            }
        }

        if flags == 1 {
            let res;
            let query = if file_name.ends_with(".json") {
                format!(
                    "COPY (SELECT row_to_json({0}) FROM {0}) TO stdout",
                    file_name.strip_suffix(".json").unwrap()
                )
            } else {
                format!("COPY {} TO STDOUT (FORMAT csv)", file_name)
            };
            match tx.copy_out(query.as_str()) {
                Ok(r) => {
                    res = r
                        .bytes()
                        .skip(offset as usize)
                        .map(|b| b.unwrap())
                        .collect::<Vec<u8>>();
                }
                Err(e) => {
                    println!("{}", e);
                    return Err(Error::NoEntry);
                }
            }
            Ok(res)
        } else {
            tx.execute(
                "INSERT INTO audit (inode, uid, event, change_begin, change_end, date) \
                VALUES ($1, $2, 'read', $3, $4, CURRENT_TIMESTAMP)",
                &[&ino_signed, &(uid as i32), &offset, &(offset + size as i64)],
            )
            .unwrap();
            match () {
                #[cfg(feature = "ext4")]
                () => {
                    match File::open(Path::new(&data_dir.as_ref().unwrap()).join(ino.to_string())) {
                        Ok(mut f) => {
                            f.seek(SeekFrom::Start(offset as u64));
                            let mut result = vec![0u8; size as usize];
                            let bytes = f.read_exact(result.as_mut_slice()).unwrap();
                            return Ok(result);
                        }
                        Err(_) => Err(Error::NoEntry),
                    }
                }
                #[cfg(feature = "bytea")]
                () => match tx.query_one(
                    "\
                SELECT SUBSTRING(data FROM $1 + 1 FOR LEAST($2, LENGTH(data)+1)) \
                FROM inodes WHERE inode = $3",
                    &[&(offset as i32), &(size as i32), &ino_signed],
                ) {
                    Ok(data) => Ok(data.try_get::<_, Vec<u8>>(0).unwrap_or(vec![])),
                    Err(E) => {
                        println!("{}", E.to_string());
                        Err(Error::NoEntry)
                    }
                },
                #[cfg(feature = "lob")]
                () => match tx.query_one(
                    "SELECT lo_get(oid, $1, $2) FROM inodes WHERE inode = $3",
                    &[&(offset as i64), &(size as i32), &ino_signed],
                ) {
                    Ok(data) => Ok(data.try_get::<_, Vec<u8>>(0).unwrap_or_default()),
                    Err(e) => {
                        println!("{}", e.to_string());
                        Err(Error::NoEntry)
                    }
                },
            }
        }
    }

    // fn rows_to_csv(table_content: &mut Rows) -> String {
    //     let mut result: Vec<String> = Vec::new();
    //     loop {
    //         let row = table_content.next();
    //         if row.is_err() {
    //             break;
    //         }
    //         let row_result = row.unwrap();
    //         match row_result {
    //             None => {
    //                 break;
    //             }
    //             Some(row) => {
    //                 let mut row_str: Vec<String> = Vec::new();
    //                 for i in 0..row.column_count() {
    //                     let value = match row.get_ref_unwrap(i) {
    //                         ValueRef::Null => String::new(),
    //                         ValueRef::Integer(v) => v.to_string(),
    //                         ValueRef::Real(v) => v.to_string(),
    //                         ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned(),
    //                         ValueRef::Blob(v) => String::from_utf8_lossy(v).into_owned(),
    //                     };
    //                     row_str.push(value);
    //                 }
    //                 result.push(row_str.join(","))
    //             }
    //         }
    //     }
    //     let result_str = result.join("\n");
    //     result_str
    // }

    // fn get_file_paths(mut self, file_name: String) -> Vec<String> {
    //     let mut stmt = self
    //         .conn
    //         .prepare(
    //             "WITH paths as (\
    //         SELECT parent, name FROM tree WHERE name = $1 \
    //         UNION ALL \
    //         SELECT tree.parent, tree.name || '/' || paths.name FROM tree, paths \
    //         WHERE paths.parent = tree.inode AND tree.inode <> 1\
    //         ) \
    //         SELECT name FROM paths WHERE parent = 1",
    //         )
    //         .unwrap();
    //     let entries_iter = stmt
    //         .query_map(&[file_name], |row| Ok(row.get::<_, String>(0)?))
    //         .unwrap();
    //
    //     let mut entries = vec![];
    //     for entry in entries_iter {
    //         entries.push(entry.unwrap());
    //     }
    //     debug!("Results: {:?}", entries);
    //
    //     entries
    //     vec![]
    // }

    /// Rename a file.
    pub fn rename(
        &mut self,
        parent: u64,
        current_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<(), Error> {
        let current_name_str = current_name.to_str().unwrap();
        let new_name_str = new_name.to_str().unwrap();
        debug!(
            "rename(parent={}, current_name={}, new_parent={}, new_name={})",
            parent, current_name_str, new_parent, new_name_str
        );

        let parent_signed = parent as i64;
        let new_parent_signed = new_parent as i64;
        self.conn
            .execute(
                "UPDATE tree SET parent = $1, name = $2 WHERE parent = $3 AND name = $4",
                &[
                    &new_parent_signed,
                    &new_name_str,
                    &parent_signed,
                    &current_name_str,
                ],
            )
            .unwrap();

        Ok(())
    }

    // pub fn fallocate(&mut self, ino: u64, offset: i64, length: i64) -> Result<(), Error> {
    //     let mut tx = self.conn.transaction().unwrap();
    //
    //     match tx
    //         .query_opt("SELECT size FROM inodes WHERE inode = $1", &[&(ino as i64)])
    //         .unwrap()
    //     {
    //         Some(row) => {
    //             let previous_size = row.get::<_, i64>(0);
    //             if previous_size < offset + length {
    //                 let extend = vec![0u8; (offset + length - previous_size) as usize];
    //                 tx.execute(
    //                     "UPDATE inodes \
    //                 SET data = COALESCE(data, '') || $2, size = $3 \
    //                 WHERE inode = $1 AND flags = 0",
    //                     &[&(ino as i64), &extend, &(offset + length)],
    //                 )
    //                 .unwrap();
    //                 tx.commit().expect("fallocate: Commit failed");
    //             }
    //             Ok(())
    //         }
    //         None => Err(Error::NoEntry),
    //     }
    // }

    pub fn symlink(
        &mut self,
        parent: u64,
        name: &OsStr,
        link: &Path,
        pid: u32,
    ) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        let link_str = link.to_str().unwrap();
        let file_attr = self
            .create_entry(parent, 0, name_str, 0, FileType::Symlink)
            .expect("Creation failed");
        let mut tx = self.conn.transaction().unwrap();
        let link_str = if link_str.starts_with("http") {
            link_str.replace("/", "\\").replace(":", "")
        } else {
            link_str.to_string()
        };
        tx.execute(
            "UPDATE inodes SET \
            data=$1, atime = CURRENT_TIMESTAMP, mtime = CURRENT_TIMESTAMP WHERE inode = $2",
            &[&link_str.as_bytes(), &(file_attr.ino as i64)],
        )
        .unwrap();
        match tx.commit() {
            Ok(_) => Ok(file_attr),
            Err(_) => Err(Error::InvalidInput),
        }
    }

    fn readlink(&mut self, ino: u64) -> Result<String, Error> {
        match self
            .conn
            .query_opt("SELECT data FROM inodes WHERE inode = $1", &[&(ino as i64)])
            .unwrap()
        {
            None => Err(Error::NoEntry),
            Some(r) => {
                let link = String::from_utf8(r.get::<_, Vec<u8>>(0)).unwrap();
                Ok(link.replace(":", ""))
            }
        }
    }

    fn link(&mut self, ino: u64, newparent: u64, newname: &OsStr) -> Result<FileAttr, Error> {
        let ino_signed = ino as i64;
        let name_str = newname.to_str().unwrap();

        let mut tx = self.conn.transaction().unwrap();
        match tx.query_opt(
            "UPDATE inodes SET nlink = nlink + 1 WHERE inode = $1 RETURNING \
        size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, flags",
            &[&ino_signed],
        ) {
            Ok(r) => match r {
                None => Err(Error::NoEntry),
                Some(row) => {
                    let fa = FileAttr {
                        ino,
                        size: row.get::<_, i64>(0) as u64,
                        blocks: row.get::<_, i32>(1) as u64,
                        atime: row.get(2),
                        mtime: row.get(3),
                        ctime: row.get(4),
                        crtime: row.get(5),
                        kind: int_to_FileType(row.get(6)),
                        perm: row.get::<_, i16>(7) as u16,
                        nlink: row.get::<_, i32>(8) as u32,
                        uid: row.get::<_, i32>(9) as u32,
                        gid: row.get::<_, i32>(10) as u32,
                        rdev: row.get::<_, i32>(11) as u32,
                        blksize: 0,
                        flags: row.get::<_, i32>(12) as u32,
                    };
                    match tx.execute(
                        "\
        INSERT INTO tree (inode, parent, name)\
        SELECT $1, $2, $3 \
        WHERE EXISTS (SELECT * FROM inodes WHERE inode = $1)",
                        &[&ino_signed, &(newparent as i64), &name_str],
                    ) {
                        Ok(c) => {
                            if c == 0 {
                                return Err(Error::NoEntry);
                            }
                        }
                        Err(e) => {
                            error!("{}", e.to_string());

                            return Err(Error::NoEntry);
                        }
                    };

                    match tx.commit() {
                        Ok(_) => Ok(fa),
                        Err(e) => {
                            error!("{}", e.to_string());
                            Err(Error::NoEntry)
                        }
                    }
                }
            },
            Err(e) => {
                error!("{}", e.to_string());
                Err(Error::NoEntry)
            }
        }
    }
}

impl Default for MemFilesystem {
    fn default() -> Self {
        MemFilesystem::new()
    }
}

impl Filesystem for MemFilesystem {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup(parent, name) {
            Ok(attr) => {
                reply.entry(&self.TTL, &attr, 0);
            }
            Err(e) => {
                reply.error(e.into());
            }
        }
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        match self.getattr(ino, req.pid()) {
            Ok(attr) => {
                info!("getattr reply with attrs = {:?}", attr);
                reply.attr(&self.TTL, &attr)
            }
            Err(e) => {
                error!("getattr reply with errno = {:?}", e);
                reply.error(e.into())
            }
        }
    }

    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let new_attrs = SetAttrRequest {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            ctime,
            fh,
            crtime,
            chgtime,
            bkuptime,
            flags,
        };

        let r = self.setattr(ino, new_attrs, req.pid());
        match r {
            Ok(fattrs) => {
                reply.attr(&self.TTL, &fattrs);
            }
            Err(e) => reply.error(e.into()),
        };
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.readlink(ino) {
            Ok(path) => reply.data(path.as_bytes()),
            Err(e) => reply.error(e.into()),
        }
    }

    // hard link support
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        match self.link(ino, newparent, newname) {
            Ok(attr) => reply.entry(&self.TTL, &attr, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.mkdir(parent, name, mode) {
            Ok(attr) => reply.entry(&self.TTL, &attr, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.unlink(parent, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.rmdir(parent, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        match self.symlink(parent, name, link, req.pid()) {
            Ok(attr) => reply.entry(&self.TTL, &attr, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        match self.rename(parent, name, newparent, newname) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn open(&mut self, _req: &Request, _ino: u64, flags: i32, reply: ReplyOpen) {
        trace!("open(ino={}, _flags={})", _ino, flags);
        reply.opened(0, flags as u32);
    }

    fn read(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read(ino, fh, offset, size, req.uid(), req.pid()) {
            Ok(slice) => reply.data(slice.as_slice()),
            Err(e) => reply.error(e.into()),
        }
    }

    fn write(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write(ino, fh, offset, data, req.uid(), req.pid(), flags) {
            Ok(bytes_written) => reply.written(bytes_written as u32),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.readdir(ino, fh) {
            Ok(entries) => {
                // Offset of 0 means no offset.
                // Non-zero offset means the passed offset has already been seen,
                // and we should start after it.
                let to_skip = if offset == 0 { 0 } else { offset + 1 } as usize;
                for (i, entry) in entries.into_iter().enumerate().skip(to_skip) {
                    if reply.add(entry.0, i as i64, entry.1, entry.2) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        };
    }

    fn setxattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let r = self.setxattr(_ino, _name, _value);
        match r {
            Ok(()) => {
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        };
    }

    fn getxattr(&mut self, _req: &Request, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
        match self.getxattr(ino, name) {
            Ok(attr) => {
                info!("getxattr reply with xattrs = {:?}", attr);
                let attr_size: u32 = u32::try_from(attr.len()).unwrap();
                if size == 0 {
                    reply.size(attr_size)
                } else if size >= attr_size {
                    reply.data(attr.as_bytes())
                } else {
                    reply.error(libc::ERANGE)
                }
            }
            Err(e) => {
                error!("getxattr reply with errno = {:?}", e);
                reply.error(e.into())
            }
        }
    }

    fn listxattr(&mut self, _req: &Request, ino: u64, size: u32, reply: ReplyXattr) {
        match self.listxattr(ino) {
            Ok(attr) => {
                info!("listxattr reply with xattrs = {:?}", attr);
                let attr_size: u32 = u32::try_from(attr.len()).unwrap();
                if size == 0 {
                    reply.size(attr_size)
                } else if size >= attr_size {
                    reply.data(&attr)
                } else {
                    reply.error(libc::ERANGE)
                }
            }
            Err(e) => {
                error!("listxattr reply with errno = {:?}", e);
                reply.error(e.into())
            }
        }
    }

    fn removexattr(&mut self, _req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let r = self.removexattr(ino, name);
        match r {
            Ok(()) => {
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        };
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        match self.create(parent, name, mode, flags) {
            Ok(attr) => reply.created(&self.TTL, &attr, 0, 0, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    // fn fallocate(
    //     &mut self,
    //     _req: &Request<'_>,
    //     ino: u64,
    //     _fh: u64,
    //     offset: i64,
    //     length: i64,
    //     _mode: i32,
    //     reply: ReplyEmpty,
    // ) {
    //     match self.fallocate(ino, offset, length) {
    //         Ok(()) => reply.ok(),
    //         Err(e) => reply.error(e.into()),
    //     }
    // }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Once;
    use std::thread;
    use tempfile::tempdir;

    static ENV_LOGGER_INIT: Once = Once::new();

    // TestContext based on https://snoozetime.github.io/2019/06/16/integration-test-diesel.html
    struct TestContext {
        db_name: String,
    }

    impl TestContext {
        fn new(db_name: &str) -> Self {
            ENV_LOGGER_INIT.call_once(|| {
                env_logger::init();
            });
            let mut client =
                Client::connect("host=localhost user=postgres password=postgres", NoTls).unwrap();
            // Batch execute does not work here, because these commands are not allowed inside of a transaction
            client
                .execute(format!("DROP DATABASE IF EXISTS {}", db_name).as_str(), &[])
                .unwrap();
            client
                .execute(format!("CREATE DATABASE {}", db_name).as_str(), &[])
                .unwrap();
            Self {
                db_name: db_name.to_string(),
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let mut client =
                Client::connect("host=localhost user=postgres password=postgres", NoTls).unwrap();
            // client.execute(format!("DROP DATABASE {}", self.db_name).as_str(), &[]).unwrap();
        }
    }

    #[test]
    fn create() {
        let test_db = "create_test";
        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);
    }

    #[test]
    fn create_readdir() {
        let test_db = "create_readdir_test";
        let test_file_name = "testFile";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        // check non existing parent
        let err = f.create(3, OsStr::new(test_file_name), 0, 0);
        assert_eq!(err.unwrap_err(), Error::ParentNotFound);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());
        let dir_content = f.readdir(1, 0).unwrap();
        let (ino, ft, name) = dir_content.get(0).unwrap();
        assert_eq!(*ino, 1);
        assert_eq!(*ft, FileType::Directory);
        assert_eq!(name, ".");

        let (ino, ft, name) = dir_content.get(1).unwrap();
        assert_eq!(*ino, 1);
        assert_eq!(*ft, FileType::Directory);
        assert_eq!(name, "..");

        let (ino, ft, name) = dir_content.get(2).unwrap();
        assert_eq!(*ino, 2);
        assert_eq!(*ft, FileType::RegularFile);
        assert_eq!(name, test_file_name);

        let err = f.create(1, OsStr::new(test_file_name), 0, 0);
        assert_eq!(err.unwrap_err(), Error::AlreadyExists)
    }

    #[test]
    fn mkdir_readdir() {
        let test_db = "mkdir_readdir_test";
        let test_ctx = TestContext::new(test_db);
        let test_file_name = "testDir";

        let mut f = MemFilesystem::new_path(test_db);

        assert!(f.mkdir(1, OsStr::new(test_file_name), 0).is_ok());
        let dir_content = f.readdir(1, 0).unwrap();
        let (ino, ft, name) = dir_content.get(0).unwrap();
        assert_eq!(*ino, 1);
        assert_eq!(*ft, FileType::Directory);
        assert_eq!(name, ".");

        let (ino, ft, name) = dir_content.get(1).unwrap();
        assert_eq!(*ino, 1);
        assert_eq!(*ft, FileType::Directory);
        assert_eq!(name, "..");

        let (ino, ft, name) = dir_content.get(2).unwrap();
        assert_eq!(*ino, 2);
        assert_eq!(*ft, FileType::Directory);
        assert_eq!(name, test_file_name);
    }

    // #[test]
    // fn fallocate() {
    //     let test_db = "fallocate_test";
    //     let test_ctx = TestContext::new(test_db);
    //     let test_file_name = "testFile";
    //
    //     let mut f = MemFilesystem::new_path(test_db);
    //
    //     assert!(f.fallocate(2, 10, 10).is_err());
    //
    //     assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());
    //     assert!(f.fallocate(2, 10, 10).is_ok());
    //     assert_eq!(f.lookup(1, test_file_name.as_ref()).unwrap().size, 20);
    //     let file_content = f.read(2, 0, 0, 20, 0).unwrap();
    //     assert!(file_content.iter().all(|b| b.eq(&0u8)));
    // }

    #[test]
    fn symlink() {
        let test_db = "symlink_test";
        let test_ctx = TestContext::new(test_db);
        let test_symlink_name = "link";
        let test_link = "https://raw.githubusercontent.com/Garfield96/btfs/master/LICENSE-MIT";
        let test_file_target = "target";
        let test_softlink_name = "solftlink";

        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }

        // External files
        assert!(f
            .symlink(1, test_symlink_name.as_ref(), test_link.as_ref(), 0)
            .is_ok());

        let content = f.read(2, 0, 0, 11, 0, 0);
        assert!(content.is_ok());
        assert_eq!(String::from_utf8(content.unwrap()).unwrap(), "MIT License");

        // Soft links
        assert!(f.create(1, OsStr::new(test_file_target), 0, 0).is_ok());

        assert!(f
            .symlink(1, test_softlink_name.as_ref(), test_file_target.as_ref(), 0)
            .is_ok());
        assert_eq!(f.readdir(1, 0).unwrap().len(), 5);
    }

    #[test]
    fn hardlinks() {
        let test_db = "hardlink_test";
        let test_ctx = TestContext::new(test_db);
        let test_file_target = "target";
        let test_hardlink_name = "solftlink";

        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }

        let previous_fa = f.create(1, OsStr::new(test_file_target), 0, 0);
        assert!(previous_fa.is_ok());
        let mut expected_fa = previous_fa.unwrap();
        expected_fa.nlink = expected_fa.nlink + 1;

        let fa = f.link(expected_fa.ino, 1, test_hardlink_name.as_ref());
        assert!(fa.is_ok());
        let fa = fa.unwrap();
        assert_eq!(f.readdir(1, 0).unwrap().len(), 4);
        assert_eq!(expected_fa, fa);
    }

    #[test]
    fn big_read_write() {
        let test_db = "big_read_write_test";
        let test_ctx = TestContext::new(test_db);
        let test_file_name = "testFile";

        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }

        use std::time::Instant;
        let now = Instant::now();

        let size = 1024 * 1024 * 128;
        let chunk_size = 1024 * 128;
        let mut test_data = Vec::with_capacity(chunk_size);
        for i in 0..chunk_size {
            test_data.push((i % 256) as u8);
        }

        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());

        let slice = test_data.as_slice();
        for i in (0..size).step_by(chunk_size) {
            assert!(f.write(2, 0, i as i64, slice, 0, 0, 0).is_ok());
        }

        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        for i in (0..size).step_by(chunk_size) {
            let safed_content = f.read(2, 0, i, chunk_size as u32, 0, 0).unwrap();
            assert_eq!(test_data, safed_content);
        }

        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
    }

    #[test]
    fn read_write() {
        let test_db = "read_write_test";
        let test_ctx = TestContext::new(test_db);
        let test_file_name = "testFile";

        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }
        check_read_write(test_file_name, &mut f);
    }

    fn check_read_write(test_file_name: &str, f: &mut MemFilesystem) {
        // check non existing file
        let err = f.read(2, 0, 0, 10, 0, 0);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        let data = "Content";
        let err = f.write(2, 0, 0, data.as_bytes(), 0, 0, 0);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());

        assert!(f.write(2, 0, 0, data.as_bytes(), 0, 0, 0).is_ok());
        let safed_content = f.read(2, 0, 0, data.len() as u32, 0, 0).unwrap();
        assert_eq!(
            safed_content.len(),
            data.len(),
            "Content: {}",
            String::from_utf8(safed_content).unwrap()
        );
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Append
        assert!(f
            .write(2, 0, data.len() as i64, data.as_bytes(), 0, 0, 0)
            .is_ok());
        let data = "ContentContent";
        let safed_content = f.read(2, 0, 0, data.len() as u32, 0, 0).unwrap();
        assert_eq!(safed_content.len(), data.len());
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Modify
        let modification = "abc";
        assert!(f.write(2, 0, 3, modification.as_bytes(), 0, 0, 0).is_ok());
        let data = "ConabctContent";
        let safed_content = f.read(2, 0, 0, data.len() as u32, 0, 0).unwrap();
        assert_eq!(safed_content.len(), data.len());
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Big data
        let data = String::from_utf8(vec![b'1'; 1000000]).unwrap();
        assert!(f.write(2, 0, 0, data.as_bytes(), 0, 0, 0).is_ok());
        assert!(f.write(2, 0, 1000000, data.as_bytes(), 0, 0, 0).is_ok());
        let safed_content = f.read(2, 0, 0, 2 * data.len() as u32, 0, 0).unwrap();
        assert_eq!(safed_content.len(), 2 * data.len());
        assert_eq!(
            safed_content.as_slice(),
            String::from_utf8(vec![b'1'; 2000000]).unwrap().as_bytes()
        );

        let new_attrs = SetAttrRequest {
            mode: Option::None,
            uid: Option::None,
            gid: Option::None,
            size: Option::Some(100),
            atime: Option::None,
            mtime: Option::None,
            ctime: Option::None,
            fh: Option::None,
            crtime: Option::None,
            chgtime: Option::None,
            bkuptime: Option::None,
            flags: Option::None,
        };
        let err = f.setattr(2, new_attrs, 0);
        // f.write(2, 0, 0, data.as_bytes(), 0);
        // f.write(2, 0, 1000000, data.as_bytes(), 0);
        // let safed_content = f.read(2, 0, 0, 2 * data.len() as u32).unwrap();
        // assert_eq!(safed_content.len(), 2 * data.len());
        // assert_eq!(safed_content.as_slice(), String::from_utf8(vec![b'1'; 2000000]).unwrap().as_bytes());
    }

    #[test]
    fn lookup() {
        let test_db = "lookup_test";
        let test_file_name = "testFile";
        let test_dir_name = "testDir";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }

        // check non existing file
        let file_attr = f.lookup(1, OsStr::new(test_file_name));
        assert_eq!(file_attr.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());
        assert!(f.mkdir(1, OsStr::new(test_dir_name), 0).is_ok());

        let file_attr = f.lookup(1, OsStr::new(test_file_name)).unwrap();
        assert_eq!(file_attr.size, 0);
        assert_eq!(file_attr.kind, FileType::RegularFile);
        let dir_attr = f.lookup(1, OsStr::new(test_dir_name)).unwrap();
        assert_eq!(dir_attr.size, 0);
        assert_eq!(dir_attr.kind, FileType::Directory);

        let data = "Content";
        assert!(f.write(2, 0, 0, data.as_bytes(), 0, 0, 0).is_ok());
        let file_attr = f.lookup(1, OsStr::new(test_file_name)).unwrap();
        assert_eq!(file_attr.size, data.len() as u64);
    }

    #[test]
    fn set_get_attr() {
        let test_db = "set_get_attr_test";
        let test_file_name = "testFile";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        let new_attrs = SetAttrRequest {
            mode: Option::Some(1),
            uid: Option::Some(2),
            gid: Option::Some(3),
            size: Option::Some(4),
            atime: Option::Some(TimeOrNow::Now),
            mtime: Option::Some(TimeOrNow::Now),
            ctime: Option::Some(SystemTime::now()),
            fh: Option::Some(7),
            crtime: Option::Some(SystemTime::now()),
            chgtime: Option::Some(SystemTime::now()),
            bkuptime: Option::Some(SystemTime::now()),
            flags: Option::Some(11),
        };
        let err = f.setattr(2, new_attrs, 0);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());
        assert!(f.setattr(2, new_attrs, 0).is_ok());

        let file_attr = f.getattr(2, 0).unwrap();
        assert_eq!(new_attrs.uid.unwrap(), file_attr.uid);
        assert_eq!(new_attrs.gid.unwrap(), file_attr.gid);
        assert_eq!(new_attrs.size.unwrap(), file_attr.size);

        let new_attrs_none = SetAttrRequest {
            mode: Option::Some(33279),
            uid: Option::None,
            gid: Option::None,
            size: Option::None,
            atime: Option::None,
            mtime: Option::None,
            ctime: Option::None,
            fh: Option::None,
            crtime: Option::None,
            chgtime: Option::None,
            bkuptime: Option::None,
            flags: Option::None,
        };
        assert!(f.setattr(2, new_attrs_none, 0).is_ok());
        let file_attr = f.getattr(2, 0).unwrap();
        assert_eq!(new_attrs.uid.unwrap(), file_attr.uid);
        assert_eq!(new_attrs.gid.unwrap(), file_attr.gid);
        assert_eq!(new_attrs.size.unwrap(), file_attr.size);
    }

    #[test]
    fn set_get_xattr() {
        let test_db = "set_get_xattr_test";
        let test_file_name_1 = "testFile";
        let test_file_name_2 = "testFile2";
        let test_file_name_3 = "testFile3";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        let err = f.setxattr(2, "depends#requires".as_ref(), "3".as_ref());
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name_1), 0, 0).is_ok());
        assert!(f.create(1, OsStr::new(test_file_name_2), 0, 0).is_ok());
        assert!(f
            .setxattr(2, "depends#requires".as_ref(), test_file_name_2.as_bytes())
            .is_ok());
        let xattr = f.getxattr(2, "depends#requires".as_ref());
        assert_eq!(test_file_name_2, xattr.unwrap());

        assert!(f.create(1, OsStr::new(test_file_name_3), 0, 0).is_ok());
        assert!(f
            .setxattr(2, "depends#requires".as_ref(), test_file_name_3.as_bytes())
            .is_ok());
        let xattr = f.getxattr(2, "depends#requires".as_ref());
        assert_eq!(
            test_file_name_2.to_owned() + "," + test_file_name_3,
            xattr.unwrap()
        );

        // f.removexattr(2, format!("requires#{}", test_file_name_2).as_ref());
        // let xattr = f.getxattr(2, "requires".as_ref());
        // assert_eq!(test_file_name_3, xattr.unwrap());

        let xattr = f.listxattr(2).unwrap();
        assert_eq!(xattr.as_slice(), "requires".as_bytes())
    }

    #[test]
    fn structured_files() {
        let test_db = "structured_files_test";
        let db_file = "db.sql";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);
        let temp_dir = tempdir().unwrap();
        if cfg!(feature = "ext4") {
            f.set_data_dir(temp_dir.path().to_str().unwrap());
        }
        assert!(f.create(1, OsStr::new(db_file), 0, 0).is_ok());

        let sql ="\
        BEGIN; \
            CREATE TABLE contacts (\
                contact_id INTEGER PRIMARY KEY,\
                first_name TEXT NOT NULL,\
                last_name TEXT NOT NULL,\
                email TEXT NOT NULL UNIQUE,\
                phone TEXT NOT NULL UNIQUE\
                ); \
            INSERT INTO contacts (contact_id, first_name, last_name, email, phone) \
            VALUES (1, 'first', 'last', 'mail', 'number' ), (2, 'first2', 'last2', 'mail2', 'number2' ); \
        COMMIT;";

        let err = f.write(2, 0, 0, sql.as_bytes(), 0, 0, 0);
        assert!(err.is_ok());
        let csv = f.read(3, 0, 0, 100, 0, 0);
        assert!(csv.is_ok());
        println!("{}", String::from_utf8(csv.unwrap()).unwrap());
        let l = f.getattr(3, 0);
        assert!(l.unwrap().size > 20);

        f.rename(1, "contacts".as_ref(), 1, "contacts.json".as_ref())
            .unwrap();
        let csv = f.read(3, 0, 0, 100, 0, 0);
        assert_eq!(csv.is_err(), false);
        println!("{}", String::from_utf8(csv.unwrap()).unwrap());
        let l = f.getattr(3, 0);
        assert!(l.unwrap().size > 20);
    }

    #[test]
    fn unlink() {
        let test_db = "unlink_test";
        let test_file_name = "testFile";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        let r = f.unlink(1, test_file_name.as_ref());
        assert_eq!(r.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_file_name);

        let r = f.unlink(1, test_file_name.as_ref());
        assert!(r.is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().len(), 2);
    }

    #[test]
    fn rmdir() {
        let test_db = "rmdir_test";
        let test_file_name = "testFile";
        let test_dir_name = "testDir";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        let r = f.rmdir(1, test_dir_name.as_ref());
        assert_eq!(r.unwrap_err(), Error::NoEntry);

        assert!(f.mkdir(1, OsStr::new(test_dir_name), 0).is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_dir_name);

        let r = f.rmdir(1, test_dir_name.as_ref());
        assert!(r.is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().len(), 2);

        assert!(f.mkdir(1, OsStr::new(test_dir_name), 0).is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_dir_name);

        assert!(f.create(2, test_file_name.as_ref(), 0, 0).is_ok());

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_dir_name);

        let r = f.readdir(2, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_file_name);

        let r = f.rmdir(1, test_dir_name.as_ref());
        assert!(r.is_err());
        assert_eq!(r.unwrap_err(), Error::NotEmpty);

        let r = f.readdir(1, 0);
        assert!(r.is_ok());
        let dir_entries = r.unwrap();
        assert_eq!(dir_entries.len(), 3);
        assert_eq!(dir_entries[2].2, test_dir_name);
    }

    #[test]
    fn transactions() {
        let test_db = "transactions_test";

        let test_ctx = TestContext::new(test_db);
        let mut f = MemFilesystem::new_path(test_db);

        assert!(f.getxattr(1, "startTransaction1".as_ref()).is_ok());
        assert!(f.getxattr(1, "startTransaction2".as_ref()).is_ok());
        assert!(f.getxattr(1, "endTransaction1".as_ref()).is_ok());
        assert!(f.getxattr(1, "endTransaction2".as_ref()).is_ok());
    }
}
