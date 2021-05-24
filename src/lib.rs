//! A simple implementation of an in-memory files-sytem written in Rust using the BTreeMap
//! data-structure.
//!
//! This code is inspired from https://github.com/bparli/bpfs and was modified to work
//! with node-replication for benchmarking.

use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::str::from_utf8;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
// Re-export reused structs from fuse:
pub use fuser::FileAttr;
pub use fuser::FileType;

use libc::{c_int, EEXIST, EINVAL, ENOENT, ENOTEMPTY};
use log::{debug, error, info, trace};
use regex::Regex;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, DatabaseName, Result, Rows};
use std::ops::Add;

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
}

/// Converts FS Errors to FUSE compatible libc errno types.
impl Into<c_int> for Error {
    fn into(self) -> c_int {
        match self {
            Error::NoEntry => ENOENT,
            Error::NotEmpty => ENOTEMPTY,
            Error::AlreadyExists => EEXIST,
            Error::InvalidInput => EINVAL,
            Error::ParentNotFound => ENOENT,
            Error::NewParentNotFound => EINVAL,
            Error::ChildNotFound => ENOENT,
        }
    }
}

fn FileTypeToInt(fileType: FileType) -> u32 {
    match fileType {
        FileType::Directory => 0,
        FileType::BlockDevice => 1,
        FileType::CharDevice => 2,
        FileType::NamedPipe => 3,
        FileType::RegularFile => 4,
        FileType::Socket => 5,
        FileType::Symlink => 6,
    }
}

fn IntToFileType(id: u32) -> FileType {
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

#[derive(Debug, Clone)]
pub struct Inode {
    name: String,
    children: BTreeMap<String, u64>,
    parent: u64,
}

impl Inode {
    fn new(name: String, parent: u64) -> Inode {
        Inode {
            name: name,
            children: BTreeMap::new(),
            parent: parent,
        }
    }
}

pub struct MemFilesystem {
    attrs: BTreeMap<u64, FileAttr>,
    inodes: BTreeMap<u64, Inode>,
    next_inode: u64,
    conn: Connection,
    TTL: Duration,
}

fn toSystemtime(sec: u64) -> SystemTime {
    let d = Duration::new(sec, 0);
    UNIX_EPOCH.add(d)
}

fn timeOrNowToUnixTime(t: TimeOrNow) -> u64 {
    match t {
        TimeOrNow::SpecificTime(specificTime) => {
            specificTime.duration_since(UNIX_EPOCH).unwrap().as_secs()
        }
        TimeOrNow::Now => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }
}

impl MemFilesystem {
    pub fn new() -> MemFilesystem {
        MemFilesystem::new_path("/tmp/fs_db.sqlite3")
    }

    pub fn new_path(path: &str) -> MemFilesystem {
        let conn = Connection::open(&path);
        let conn = match conn {
            Ok(conn) => conn,
            Err(e) => {
                println!("DB connection failed: error: {}", e);
                process::exit(1);
            }
        };

        conn.execute(
            "CREATE TABLE if not exists tree (
                  inode              INTEGER PRIMARY KEY,
                  parent              INTEGER,
                  name                  TEXT NOT NULL
                  )",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE if not exists inodes (
                    inode              INTEGER PRIMARY KEY,
                    data    BLOB NOT NULL DEFAULT (x''),
                    size INTEGER NOT NULL DEFAULT 0,
                    blocks INTEGER NOT NULL DEFAULT 0,
                    atime INTEGER NOT NULL,
                    mtime INTEGER NOT NULL,
                    ctime INTEGER NOT NULL,
                    crtime INTEGER NOT NULL,
                    kind INTEGER NOT NULL,
                    perm INTEGER NOT NULL DEFAULT 511,
                    nlink INTEGER NOT NULL DEFAULT 1,
                    uid INTEGER NOT NULL DEFAULT 0,
                    gid INTEGER NOT NULL DEFAULT 0,
                    rdev INTEGER NOT NULL DEFAULT 0,
                    flags INTEGER NOT NULL DEFAULT 0
                  )",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE if not exists dependencies (
                  inode              INTEGER,
                  dep_inode            INTEGER NOT NULL,
                  type                  TEXT NOT NULL
                  )",
            [],
        )
        .unwrap();

        let root = Inode::new("/".to_string(), 1 as u64);

        let mut attrs = BTreeMap::new();
        let mut inodes = BTreeMap::new();
        let ts = SystemTime::now();
        let attr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            padding: 0,
            flags: 0,
        };
        attrs.insert(1, attr);
        inodes.insert(1, root);

        conn.execute("REPLACE INTO inodes (inode, atime, mtime, ctime, crtime, kind) VALUES (1, ?2, ?2, ?2, ?2, ?1)",
                     params![FileTypeToInt(FileType::Directory), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()]).unwrap();
        conn.execute("REPLACE INTO tree VALUES (1, 1, '/')", [])
            .unwrap();

        MemFilesystem {
            attrs: attrs,
            inodes: inodes,
            next_inode: 2,
            conn: conn,
            TTL: Duration::new(1, 0),
        }
    }

    /// Generates inode numbers.
    fn get_next_ino(&mut self) -> u64 {
        self.next_inode += 1;
        self.next_inode
    }

    pub fn getattr(&mut self, ino: u64) -> Result<FileAttr, Error> {
        debug!("getattr(ino={})", ino);
        // DB version
        let mut stmt = self.conn.prepare(
            "SELECT inode, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, flags \
            FROM inodes WHERE inode = ?1"
        ).unwrap();
        match stmt.query_row(params![ino], |row| {
            Ok(FileAttr {
                ino: row.get(0)?,
                size: row.get(1)?,
                blocks: row.get(2)?,
                atime: toSystemtime(row.get::<_, u64>(3).unwrap()),
                mtime: toSystemtime(row.get::<_, u64>(4).unwrap()),
                ctime: toSystemtime(row.get::<_, u64>(5).unwrap()),
                crtime: toSystemtime(row.get::<_, u64>(6).unwrap()),
                kind: IntToFileType(row.get(7)?),
                perm: row.get(8)?,
                nlink: row.get(9)?,
                uid: row.get(10)?,
                gid: row.get(11)?,
                rdev: row.get(12)?,
                blksize: 0,
                padding: 0,
                flags: row.get(13)?,
            })
        }) {
            Ok(mut res) => {
                stmt.finalize();
                if res.flags == 1 {
                    res.size = self.read(ino, 0, 0, 0).unwrap().len() as u64;
                }
                Ok(res)
            }
            Err(_) => Err(Error::NoEntry),
        }
    }

    /// Retrieves the extended attributes of a file.
    pub fn getxattr(&mut self, ino: u64, name: &OsStr) -> Result<String, Error> {
        let name = name.to_str().unwrap();
        debug!("getxattr(ino={}, name={})", ino, name);

        if name.starts_with("dependents") {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT name FROM dependencies, tree \
                WHERE dependencies.inode = tree.inode AND dep_inode = ?1",
                )
                .unwrap();
            let table_content = stmt
                .query_map(params![ino], |row| row.get::<_, String>(0))
                .unwrap();
            let result: Vec<String> = table_content.map(|e| e.unwrap()).collect();
            return Ok(result.join(","));
        }

        if name.starts_with("sql#") {
            let stmt = self.conn.prepare(name.strip_prefix("sql#").unwrap());
            if stmt.is_err() {
                debug!("{}", stmt.unwrap_err().to_string());
                return Err(Error::InvalidInput);
            }
            let mut stmt = stmt.unwrap();
            let mut table_content = stmt.query([]).unwrap();
            return Ok(MemFilesystem::rows_to_csv(&mut table_content));
        }

        let mut stmt = self
            .conn
            .prepare(
                "SELECT name FROM tree, dependencies \
            WHERE type = ?1 AND dependencies.inode = ?2 AND tree.inode = dependencies.dep_inode",
            )
            .unwrap();
        let result = stmt
            .query_map(params![name, ino], |row| {
                Ok(row.get::<_, String>(0).unwrap())
            })
            .unwrap();

        Ok(result
            .map(|x| x.unwrap())
            .collect::<Vec<String>>()
            .join(","))
    }

    /// Lists all extended attributes of a file.
    pub fn listxattr(&mut self, ino: u64) -> Result<Vec<u8>, Error> {
        debug!("listxattr(ino={})", ino);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT type FROM dependencies \
            WHERE inode = ?1",
            )
            .unwrap();
        let result = stmt
            .query_map(params![ino], |row| Ok(row.get::<_, String>(0).unwrap()))
            .unwrap();
        let result_str: String = result
            .map(|x| x.unwrap())
            .collect::<Vec<String>>()
            .join(",");
        Ok(result_str.as_bytes().iter().cloned().collect())
    }

    fn removexattr(&mut self, ino: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        let mut parts = name_str.split("#");

        let type_str = parts.next().unwrap();
        let name_str = parts.next().unwrap();
        let meta = fs::metadata(name_str).unwrap();
        self.conn
            .execute(
                "\
        DELETE FROM dependencies WHERE inode = ?1 AND dep_inode = ?2 AND type = ?3;",
                params![ino, meta.ino(), type_str],
            )
            .unwrap();
        Ok(())
    }

    /// Updates the attributes on an inode with values in `new_attrs`.
    pub fn setattr(&mut self, ino: u64, new_attrs: SetAttrRequest) -> Result<FileAttr, Error> {
        debug!("setattr(ino={}, new_attrs={:?})", ino, new_attrs);
        if new_attrs.size.is_some() {
            let blob = self
                .conn
                .blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false);
            if blob.is_err() {
                return Err(Error::NoEntry);
            }

            let mut blob = blob.unwrap();
            let size = blob.size();

            let mut buf = vec![
                0;
                if new_attrs.size.unwrap() < size as u64 {
                    new_attrs.size.unwrap()
                } else {
                    size as u64
                } as usize
            ];
            blob.read_exact(buf.as_mut());
            self.conn.execute("UPDATE inodes SET data = zeroblob(?2), size = ?2, atime = ?3, mtime = ?3 WHERE inode = ?1",
                                  params![ino, new_attrs.size, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()]);
            blob.reopen(ino as i64);
            blob.write_all_at(buf.as_slice(), 0).unwrap();
        }

        let arg = vec![
            (if new_attrs.mode.is_some() {
                format!("perm = {}", new_attrs.mode.unwrap_or(0))
            } else {
                String::new()
            }),
            (if new_attrs.uid.is_some() {
                format!("uid = {}", new_attrs.uid.unwrap_or(0))
            } else {
                String::new()
            }),
            (if new_attrs.gid.is_some() {
                format!("gid = {}", new_attrs.gid.unwrap_or(0))
            } else {
                String::new()
            }),
            (if new_attrs.atime.is_some() {
                format!(
                    "atime = {}",
                    timeOrNowToUnixTime(new_attrs.atime.unwrap_or(TimeOrNow::Now))
                )
            } else {
                String::new()
            }),
            (if new_attrs.mtime.is_some() {
                format!(
                    "mtime = {}",
                    timeOrNowToUnixTime(new_attrs.mtime.unwrap_or(TimeOrNow::Now))
                )
            } else {
                String::new()
            }),
            (if new_attrs.crtime.is_some() {
                format!(
                    "crtime = {}",
                    new_attrs
                        .crtime
                        .unwrap_or(SystemTime::now())
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                )
            } else {
                String::new()
            }),
            (if new_attrs.size.is_some() {
                format!("size = {}", new_attrs.size.unwrap_or(0))
            } else {
                String::new()
            }),
        ]
        .into_iter()
        .filter(|x| !x.is_empty())
        .collect::<Vec<String>>()
        .join(", ");
        let sql = format!("UPDATE inodes SET {} WHERE inode = ?1", arg);
        let err = self.conn.execute(sql.as_str(), params![ino]);

        if err.is_err() {
            debug!("{}", sql);
        }
        self.getattr(ino)
    }

    /// Updates the extended attributes on an file.
    pub fn setxattr(&mut self, ino: u64, name: &OsStr, value: &[u8]) -> Result<(), Error> {
        let name = name.to_str().unwrap();
        let value = from_utf8(value).unwrap();
        debug!("setxattr(ino={}, name={}, value={})", ino, name, value);

        let changed_rows = self
            .conn
            .execute(
                "\
         INSERT INTO dependencies (inode, dep_inode, type)  \
         SELECT ?1, tree.inode, ?3 FROM tree \
         WHERE EXISTS (SELECT * FROM inodes WHERE inode = ?1) AND tree.name = ?2",
                params![ino, value, name],
            )
            .unwrap();

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
        debug!("readdir(ino={}, fh={})", ino, _fh);
        let mut entries: Vec<(u64, FileType, String)> = Vec::with_capacity(32);
        entries.push((ino, FileType::Directory, String::from(".")));

        // DB version
        let mut stmt = self.conn.prepare(
            "SELECT inode, ?2 as kind, '..' as name FROM tree WHERE inode = ?1 \
            UNION ALL \
            SELECT inodes.inode, kind, name FROM inodes, tree WHERE inodes.inode = tree.inode and tree.parent = ?1 and name <> '/'"
        ).unwrap();
        let entries_iter = stmt
            .query_map(params![ino, FileTypeToInt(FileType::Directory)], |row| {
                Ok((
                    row.get::<_, InodeId>(0)?,
                    IntToFileType(row.get(1).unwrap()),
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap();

        for entry in entries_iter {
            entries.push(entry.unwrap());
        }
        debug!("Results: {:?}", entries);

        Ok(entries)
    }

    pub fn lookup(&mut self, parent: u64, name: &OsStr) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("lookup(parent={}, name={})", parent, name_str);

        let mut stmt = self.conn.prepare(
            "SELECT inodes.inode, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, flags \
            FROM inodes, tree WHERE inodes.inode = tree.inode AND parent = ?1 AND name = ?2"
        ).unwrap();
        match stmt.query_row(params![parent, name_str], |row| {
            Ok(FileAttr {
                ino: row.get(0)?,
                size: row.get(1)?,
                blocks: row.get(2)?,
                atime: toSystemtime(row.get(3).unwrap()),
                mtime: toSystemtime(row.get(4).unwrap()),
                ctime: toSystemtime(row.get(5).unwrap()),
                crtime: toSystemtime(row.get(6).unwrap()),
                kind: IntToFileType(row.get(7)?),
                perm: row.get(8)?,
                nlink: row.get(9)?,
                uid: row.get(10)?,
                gid: row.get(11)?,
                rdev: row.get(12)?,
                blksize: 0,
                padding: 0,
                flags: row.get(13)?,
            })
        }) {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::NoEntry),
        }
    }

    pub fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        debug!("rmdir(parent={}, name={})", parent, name_str);

        let mut tx = self.conn.transaction().unwrap();
        {
            let number_of_children = tx.query_row("SELECT count(*) FROM tree WHERE parent = (SELECT inode FROM tree WHERE parent = ?1 AND name = ?2)",
            params![parent, name_str], |row| row.get::<_,u64>(0) );
            match number_of_children {
                Ok(n) => {
                    if n > 0 {
                        return Err(Error::NotEmpty);
                    }
                }
                Err(_) => return Err(Error::NoEntry),
            }
            let changed_rows = tx.execute("\
        UPDATE inodes SET nlink = nlink - 1 WHERE inode = (SELECT inode FROM tree WHERE parent = ?1 AND name = ?2);",
                              params![parent, name_str]).unwrap();

            if changed_rows == 0 {
                tx.rollback();
                return Err(Error::NoEntry);
            }

            tx.execute(
                "\
        DELETE FROM tree WHERE parent = ?1 AND name = ?2;",
                params![parent, name_str],
            )
            .unwrap();
            tx.execute("DELETE FROM inodes WHERE nlink = 0", [])
                .unwrap();
        }
        tx.commit();
        Ok(())
    }

    pub fn mkdir(&mut self, parent: u64, name: &OsStr, mode: u32) -> Result<FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("mkdir(parent={}, name={})", parent, name_str);

        self.create_entry(parent, 0, name_str, mode, FileType::Directory)
    }

    pub fn unlink(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        debug!("unlink(parent={}, name={})", parent, name_str);

        let tx = self.conn.transaction().unwrap();
        {
            let changed_rows = tx.execute("\
        UPDATE inodes SET nlink = nlink - 1 WHERE inode = (SELECT inode FROM tree WHERE parent = ?1 AND name = ?2);",
                               params![parent, name_str]).unwrap();
            if changed_rows == 0 {
                tx.rollback();
                return Err(Error::NoEntry);
            }
            tx.execute(
                "\
        DELETE FROM tree WHERE parent = ?1 AND name = ?2;",
                params![parent, name_str],
            )
            .unwrap();
            tx.execute("DELETE FROM inodes WHERE nlink = 0", [])
                .unwrap();
        }
        tx.commit();
        Ok(())
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
        let new_inode_nr: u64 = self
            .conn
            .query_row(
                "SELECT max(inode) as max_index FROM inodes",
                [],
                |max_index| Ok(max_index.get::<_, u64>(0)?),
            )
            .unwrap()
            + 1;

        let tx = self.conn.transaction().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let err = tx.execute(
            "\
        INSERT INTO inodes (inode, atime, mtime, ctime, crtime, kind, flags, perm) \
        SELECT ?3, ?2, ?2, ?2, ?2, ?1, ?6, ?7 \
        WHERE EXISTS (SELECT * FROM inodes WHERE inodes.inode = ?4 AND inodes.kind = ?5)",
            params![
                FileTypeToInt(file_type),
                now,
                new_inode_nr,
                parent,
                FileTypeToInt(FileType::Directory),
                flags,
                mode
            ],
        );
        if err.unwrap() == 0 {
            tx.rollback();
            return Err(Error::ParentNotFound);
        }

        let err = tx.execute(
            "\
        INSERT INTO tree (inode, parent, name)\
        SELECT ?1, ?2, ?3 \
        WHERE NOT EXISTS (SELECT * FROM tree WHERE tree.name = ?3 AND parent = ?2)",
            params![new_inode_nr, parent, name_str.to_string()],
        );
        if err.unwrap() == 0 {
            tx.rollback();
            return Err(Error::AlreadyExists);
        }
        tx.commit();
        let ts_now = SystemTime::now();
        Ok(FileAttr {
            ino: new_inode_nr,
            size: 0,
            blocks: 0,
            atime: ts_now,
            mtime: ts_now,
            ctime: ts_now,
            crtime: ts_now,
            kind: file_type,
            perm: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            padding: 0,
            flags: flags as u32,
        })
    }

    pub fn write(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: i32,
    ) -> Result<u64, Error> {
        debug!("write(ino={}, fh={}, offset={})", ino, fh, offset);

        let mut size = 0;

        // Structured Files
        let query_result = self.conn.query_row(
            "SELECT parent, name FROM tree WHERE inode = ?1",
            params![ino],
            |row| {
                Ok((
                    row.get::<_, u64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                ))
            },
        );

        let mut parent;
        let mut file_name;
        match query_result {
            Ok((p, n)) => {
                parent = p;
                file_name = n;
            }
            Err(_) => return Err(Error::NoEntry),
        }

        if file_name.ends_with(".sql") {
            let re = Regex::new(r"CREATE TABLE ([[:alpha:]]+)").unwrap();
            let data_str = String::from_utf8_lossy(data);
            for cap in re.captures_iter(data_str.as_ref()) {
                let f = self.create(parent, (&cap[1]).as_ref(), 0, 1);
                let ino_str = format!("{}", ino);
                self.write(f.unwrap().ino, 0, 0, ino_str.as_bytes(), 1);
                println!("{}", &cap[1]);
            }
            match self.conn.execute_batch(data_str.as_ref()) {
                Ok(_) => {}
                Err(e) => {
                    println!("{}", e);
                    return Err(Error::InvalidInput);
                }
            };
        }

        // Read csv

        // CREATE VIRTUAL TABLE vtab USING csv('<file>', HAS_HEADERS);
        // CREATE TABLE <table> AS SELECT * from vtab;

        // Regular Files
        let mut tx = self.conn.transaction().unwrap();
        {
            let mut blob = tx.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false);
            if blob.is_err() {
                return Err(Error::NoEntry);
            }

            let mut blob = blob.unwrap();

            if blob.size() > (offset + data.len() as i64) as i32 {
                blob.write_all_at(data, offset as usize);
                // Update file attributes
                tx.execute(
                    "UPDATE inodes SET atime = ?1, mtime = ?1 WHERE inode = ?2",
                    params![
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        ino
                    ],
                )
                .unwrap();
            } else {
                let mut buf = vec![0; offset as usize + data.len()];
                blob.read_exact(buf.as_mut());
                buf.splice((offset as usize).., data.iter().cloned());
                tx.execute("UPDATE inodes SET data = zeroblob(?2), size = ?2, atime = ?3, mtime = ?3 WHERE inode = ?1",
                           params![ino, offset + data.len() as i64, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()]);
                blob.reopen(ino as i64);
                // let mut blob = self.conn.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false).unwrap();
                blob.write_all_at(buf.as_slice(), 0).unwrap();
            }

            size = blob.size();
        }
        tx.commit();

        trace!(
            "write done(ino={}, wrote={}, offset={}, new size={})",
            ino,
            data.len(),
            offset,
            size
        );

        Ok(data.len() as u64)
    }

    pub fn read(&mut self, ino: u64, fh: u64, offset: i64, size: u32) -> Result<Vec<u8>, Error> {
        debug!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino, fh, offset, size
        );

        let query_result = self.conn.query_row("SELECT flags, name FROM tree, inodes WHERE tree.inode = inodes.inode AND tree.inode = ?1",
                                                                      params![ino], |row| {Ok((row.get::<_, u64>(0).unwrap(), row.get::<_, String>(1).unwrap()))});
        let mut flags;
        let mut file_name;
        match query_result {
            Ok((f, n)) => {
                flags = f;
                file_name = n;
            }
            Err(_) => return Err(Error::NoEntry),
        }

        if flags == 1 {
            let mut stmt = self
                .conn
                .prepare(&*format!("SELECT * FROM {}", file_name))
                .unwrap();
            let mut table_content = stmt.query([]).unwrap();
            let result_str = MemFilesystem::rows_to_csv(&mut table_content);
            println!("{}", result_str);
            Ok(Vec::from(result_str.as_bytes()))
        } else {
            match self
                .conn
                .blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false)
            {
                Ok(blob) => {
                    let mut buf = vec![b'0'; size as usize];
                    blob.read_at(buf.as_mut(), offset as usize);
                    Ok(buf)
                }
                Err(_) => Err(Error::NoEntry),
            }
        }
    }

    fn rows_to_csv(table_content: &mut Rows) -> String {
        let mut result: Vec<String> = Vec::new();
        loop {
            let row = table_content.next();
            if row.is_err() {
                break;
            }
            let row_result = row.unwrap();
            match row_result {
                None => {
                    break;
                }
                Some(row) => {
                    let mut row_str: Vec<String> = Vec::new();
                    for i in 1..row.column_count() {
                        let value = match row.get_ref_unwrap(i) {
                            ValueRef::Null => String::new(),
                            ValueRef::Integer(v) => v.to_string(),
                            ValueRef::Real(v) => v.to_string(),
                            ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned(),
                            ValueRef::Blob(v) => String::from_utf8_lossy(v).into_owned(),
                        };
                        row_str.push(value);
                    }
                    result.push(row_str.join(","))
                }
            }
        }
        let result_str = result.join("\n");
        result_str
    }

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

        self.conn.execute(
            "UPDATE tree SET parent = ?1, name = ?2 WHERE parent = ?3 AND name = ?4",
            params![new_parent, new_name_str, parent, current_name_str],
        );

        Ok(())
    }

    pub fn fallocate(&mut self, ino: u64, offset: i64, length: i64) -> Result<(), Error> {
        let mut tx = self.conn.transaction().unwrap();
        {
            let mut blob = tx.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false);
            if blob.is_err() {
                return Err(Error::NoEntry);
            }

            let mut blob = blob.unwrap();

            if blob.size() > (offset + length) as i32 {
                // truncate
            } else {
                let mut buf = vec![0; (offset + length) as usize];
                blob.read_exact(buf.as_mut());
                tx.execute("UPDATE inodes SET data = zeroblob(?2), size = ?2, atime = ?3, mtime = ?3 WHERE inode = ?1",
                           params![ino, offset + length, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()]);
                blob.reopen(ino as i64);
                // let mut blob = self.conn.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false).unwrap();
                blob.write_all_at(buf.as_slice(), 0).unwrap();
            }
        }
        tx.commit().expect("fallocate: Commit failed");
        Ok(())
    }
}

impl Default for MemFilesystem {
    fn default() -> Self {
        MemFilesystem::new()
    }
}

impl Filesystem for MemFilesystem {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.getattr(ino) {
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

    fn setattr(
        &mut self,
        _req: &Request,
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

        let r = self.setattr(ino, new_attrs);
        match r {
            Ok(fattrs) => {
                reply.attr(&self.TTL, &fattrs);
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

    fn removexattr(&mut self, _req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let r = self.removexattr(ino, name);
        match r {
            Ok(()) => {
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        };
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

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.rmdir(parent, name) {
            Ok(()) => reply.ok(),
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

    fn open(&mut self, _req: &Request, _ino: u64, flags: i32, reply: ReplyOpen) {
        trace!("open(ino={}, _flags={})", _ino, flags);
        reply.opened(0, flags as u32);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.unlink(parent, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
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

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write(ino, fh, offset, data, flags) {
            Ok(bytes_written) => reply.written(bytes_written as u32),
            Err(e) => reply.error(e.into()),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read(ino, fh, offset, size) {
            Ok(slice) => reply.data(slice.as_slice()),
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

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        match self.fallocate(ino, offset, length) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn memfs_create_readdir() {
        let test_db_path = "/tmp/test1.db";
        let test_file_name = "testFile";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);
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
    fn memfs_mkdir_readdir() {
        let test_db_path = "/tmp/test2.db";
        let test_file_name = "testDir";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);
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

    #[test]
    fn memfs_read_write() {
        let test_db_path = "/tmp/test3.db";
        let test_file_name = "testFile";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

        // check non existing file
        let err = f.read(2, 0, 0, 10);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        let data = "Content";
        let err = f.write(2, 0, 0, data.as_bytes(), 0);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());

        assert!(f.write(2, 0, 0, data.as_bytes(), 0).is_ok());
        let safed_content = f.read(2, 0, 0, data.len() as u32).unwrap();
        assert_eq!(safed_content.len(), data.len());
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Append
        assert!(f.write(2, 0, data.len() as i64, data.as_bytes(), 0).is_ok());
        let data = "ContentContent";
        let safed_content = f.read(2, 0, 0, data.len() as u32).unwrap();
        assert_eq!(safed_content.len(), data.len());
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Modify
        let modification = "abc";
        assert!(f.write(2, 0, 3, modification.as_bytes(), 0).is_ok());
        let data = "ConabctContent";
        let safed_content = f.read(2, 0, 0, data.len() as u32).unwrap();
        assert_eq!(safed_content.len(), data.len());
        assert_eq!(safed_content.as_slice(), data.as_bytes());

        // Big data
        let data = String::from_utf8(vec![b'1'; 1000000]).unwrap();
        assert!(f.write(2, 0, 0, data.as_bytes(), 0).is_ok());
        assert!(f.write(2, 0, 1000000, data.as_bytes(), 0).is_ok());
        let safed_content = f.read(2, 0, 0, 2 * data.len() as u32).unwrap();
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
        let err = f.setattr(2, new_attrs);
        // f.write(2, 0, 0, data.as_bytes(), 0);
        // f.write(2, 0, 1000000, data.as_bytes(), 0);
        // let safed_content = f.read(2, 0, 0, 2 * data.len() as u32).unwrap();
        // assert_eq!(safed_content.len(), 2 * data.len());
        // assert_eq!(safed_content.as_slice(), String::from_utf8(vec![b'1'; 2000000]).unwrap().as_bytes());
    }

    #[test]
    fn memfs_lookup() {
        let test_db_path = "/tmp/test4.db";
        let test_file_name = "testFile";
        let test_dir_name = "testDir";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

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
        assert!(f.write(2, 0, 0, data.as_bytes(), 0).is_ok());
        let file_attr = f.lookup(1, OsStr::new(test_file_name)).unwrap();
        assert_eq!(file_attr.size, data.len() as u64);
    }

    #[test]
    fn memfs_set_get_attr() {
        let test_db_path = "/tmp/test5.db";
        let test_file_name = "testFile";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);
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
        let err = f.setattr(2, new_attrs);
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name), 0, 0).is_ok());
        assert!(f.setattr(2, new_attrs).is_ok());

        let file_attr = f.getattr(2).unwrap();
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
        assert!(f.setattr(2, new_attrs_none).is_ok());
        let file_attr = f.getattr(2).unwrap();
        assert_eq!(new_attrs.uid.unwrap(), file_attr.uid);
        assert_eq!(new_attrs.gid.unwrap(), file_attr.gid);
        assert_eq!(new_attrs.size.unwrap(), file_attr.size);
    }

    #[test]
    fn memfs_set_get_xattr() {
        let test_db_path = "/tmp/test6.db";
        let test_file_name_1 = "testFile";
        let test_file_name_2 = "testFile2";
        let test_file_name_3 = "testFile3";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

        let err = f.setxattr(2, "requires".as_ref(), "3".as_ref());
        assert_eq!(err.unwrap_err(), Error::NoEntry);

        assert!(f.create(1, OsStr::new(test_file_name_1), 0, 0).is_ok());
        assert!(f.create(1, OsStr::new(test_file_name_2), 0, 0).is_ok());
        assert!(f
            .setxattr(2, "requires".as_ref(), test_file_name_2.as_bytes())
            .is_ok());
        let xattr = f.getxattr(2, "requires".as_ref());
        assert_eq!(test_file_name_2, xattr.unwrap());

        assert!(f.create(1, OsStr::new(test_file_name_3), 0, 0).is_ok());
        assert!(f
            .setxattr(2, "requires".as_ref(), test_file_name_3.as_bytes())
            .is_ok());
        let xattr = f.getxattr(2, "requires".as_ref());
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
    fn memfs_structured_files() {
        let test_db_path = "/tmp/test7.db";
        let db_file = "db.sql";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

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
            INSERT INTO contacts (first_name, last_name, email, phone) VALUES ('first', 'last', 'mail', 'number' ), ('first2', 'last2', 'mail2', 'number2' ); \
        COMMIT;";

        let err = f.write(2, 0, 0, sql.as_bytes(), 0);
        assert_eq!(err.is_err(), false);
        let csv = f.read(3, 0, 0, 100);
        assert_eq!(csv.is_err(), false);
        let l = f.getattr(3);
        assert!(l.unwrap().size > 20);
    }

    #[test]
    fn memfs_unlink() {
        let test_db_path = "/tmp/test8.db";
        let test_file_name = "testFile";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

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
    fn memfs_rmdir() {
        let test_db_path = "/tmp/test9.db";
        let test_file_name = "testFile";
        let test_dir_name = "testDir";

        match fs::remove_file(test_db_path) {
            Err(ref e) if e.kind() != std::io::ErrorKind::NotFound => {
                assert!(false, "{}", e.to_string())
            }
            _ => {}
        }
        let mut f = MemFilesystem::new_path(test_db_path);

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
}
