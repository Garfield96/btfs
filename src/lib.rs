//! A simple implementation of an in-memory files-sytem written in Rust using the BTreeMap
//! data-structure.
//!
//! This code is inspired from https://github.com/bparli/bpfs and was modified to work
//! with node-replication for benchmarking.

use std::{iter, process, fs};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::ops::Add;
use std::str::from_utf8;
use std::time::{SystemTime, UNIX_EPOCH};
use std::os::unix::fs::MetadataExt;

use fuse::{Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request};
// Re-export reused structs from fuse:
pub use fuse::FileAttr;
pub use fuse::FileType;

use libc::{c_int, EEXIST, EINVAL, ENOENT, ENOTEMPTY};
use log::{debug, error, info, trace};
use rusqlite::{Connection, DatabaseName, OptionalExtension, params, Result};
use time::Timespec;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

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
    pub atime: Option<Timespec>,
    pub mtime: Option<Timespec>,
    pub fh: Option<u64>,
    pub crtime: Option<Timespec>,
    pub chgtime: Option<Timespec>,
    pub bkuptime: Option<Timespec>,
    pub flags: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct MemFile {
    bytes: Vec<u8>,
    xattr: BTreeMap<String, String>,
}

impl MemFile {
    pub fn new() -> MemFile {
        MemFile { bytes: Vec::new(), xattr: BTreeMap::new() }
    }

    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn update(&mut self, new_bytes: &[u8], offset: i64) -> u64 {
        let offset: usize = offset as usize;

        if offset >= self.bytes.len() {
            // extend with zeroes until we are at least at offset
            self.bytes
                .extend(iter::repeat(0).take(offset - self.bytes.len()));
        }

        if offset + new_bytes.len() > self.bytes.len() {
            self.bytes.splice(offset.., new_bytes.iter().cloned());
        } else {
            self.bytes
                .splice(offset..offset + new_bytes.len(), new_bytes.iter().cloned());
        }

        debug!(
            "update(): len of new bytes is {}, total len is {}, offset was {}",
            new_bytes.len(),
            self.size(),
            offset
        );

        new_bytes.len() as u64
    }

    fn truncate(&mut self, size: u64) {
        self.bytes.truncate(size as usize);
    }
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
    files: BTreeMap<u64, MemFile>,
    attrs: BTreeMap<u64, FileAttr>,
    inodes: BTreeMap<u64, Inode>,
    next_inode: u64,
    conn: Connection,
}

impl MemFilesystem {
    pub fn new() -> MemFilesystem {
        let path = "/tmp/fs_db.sqlite3";
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
        ).unwrap();

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
        ).unwrap();

        conn.execute(
            "CREATE TABLE if not exists dependencies (
                  inode              INTEGER,
                  dep_inode            INTEGER NOT NULL,
                  type                  TEXT NOT NULL
                  )",
            [],
        ).unwrap();

        let files = BTreeMap::new();

        let root = Inode::new("/".to_string(), 1 as u64);

        let mut attrs = BTreeMap::new();
        let mut inodes = BTreeMap::new();
        let ts = time::now().to_timespec();
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
            flags: 0,
        };
        attrs.insert(1, attr);
        inodes.insert(1, root);

        conn.execute("REPLACE INTO inodes (inode, atime, mtime, ctime, crtime, kind) VALUES (1, ?2, ?2, ?2, ?2, ?1)",
                     params![FileTypeToInt(FileType::Directory), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()]).unwrap();
        conn.execute("REPLACE INTO tree VALUES (1, 1, '/')", []).unwrap();

        MemFilesystem {
            files: files,
            attrs: attrs,
            inodes: inodes,
            next_inode: 2,
            conn: conn,
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
                atime: Timespec { sec: row.get(3)?, nsec: 0 },
                mtime: Timespec { sec: row.get(4)?, nsec: 0 },
                ctime: Timespec { sec: row.get(5)?, nsec: 0 },
                crtime: Timespec { sec: row.get(6)?, nsec: 0 },
                kind: IntToFileType(row.get(7)?),
                perm: row.get(8)?,
                nlink: row.get(9)?,
                uid: row.get(10)?,
                gid: row.get(11)?,
                rdev: row.get(12)?,
                flags: row.get(13)?,
            })
        }) {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::NoEntry)
        }

        //self.attrs.get(&ino).ok_or(Error::NoEntry)
    }

    /// Retrieves the extended attributes of a file.
    pub fn getxattr(&mut self, ino: u64, name: &OsStr) -> Result<String, Error> {
        let name = name.to_str().unwrap();
        debug!("getxattr(ino={}, name={})", ino, name);


        let mut stmt = self.conn.prepare(
            "SELECT name FROM tree, dependencies \
            WHERE type = ?1 AND tree.inode = ?2 AND tree.inode = dependencies.inode"
        ).unwrap();
        let result = stmt.query_map(params![name, ino], |row| {
            Ok(row.get::<_, String>(0).unwrap())
        }).unwrap();

        let mut deps = String::from("");
        for dep in result {
            deps = deps.add(dep.unwrap().as_str());
        }
        Ok(deps)

        //let memfile = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;

        //Ok(memfile.xattr.get(name).unwrap().as_ref())
    }

    /// Lists all extended attributes of a file.
    pub fn listxattr(&mut self, ino: u64) -> Result<Vec<u8>, Error> {
        debug!("listxattr(ino={})", ino);
        let mut result: Vec<u8> = Vec::new();
        let keys = self.files
            .get(&ino)
            .unwrap().xattr
            .keys();
        for key in keys {
            result.append(&mut key.clone().into_bytes());
            result.push(0);
        }

        // let mut stmt = conn.prepare("SELECT id, dependency, type FROM dependencies WHERE id=(?)")?;
        // let person_iter = stmt.query_map([], |row| {
        //     Ok(Person {
        //         id: row.get(0)?,
        //         name: row.get(1)?,
        //         data: row.get(2)?,
        //     })
        // })?;
        println!("{}", from_utf8(result.as_slice()).unwrap());
        Ok(result)
    }

    fn removexattr(
        &mut self,
        ino: u64,
        name: &OsStr,
    ) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        let mut parts = name_str.split("#");

        let type_str = parts.next().unwrap();
        let name_str = parts.next().unwrap();
        let meta = fs::metadata(name_str).unwrap();
        self.conn.execute("\
        DELETE FROM dependencies WHERE inode = ?1 AND dep_inode = ?2 AND type = ?3;",
                          params![ino, meta.ino(), type_str]).unwrap();
        Ok(())
    }

    /// Updates the attributes on an inode with values in `new_attrs`.
    pub fn setattr(&mut self, ino: u64, new_attrs: SetAttrRequest) -> Result<&FileAttr, Error> {
        debug!("setattr(ino={}, new_attrs={:?})", ino, new_attrs);
        let mut file_attrs = self.attrs.get_mut(&ino).ok_or(Error::NoEntry)?;

        // This should be first, so if we don't find the file don't update other attributes:
        match new_attrs.size {
            Some(new_size) => {
                let memfile = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;
                memfile.truncate(new_size);
                file_attrs.size = new_size;
            }
            _ => (),
        }

        new_attrs.uid.map(|new_uid| file_attrs.uid = new_uid);
        new_attrs.gid.map(|new_gid| file_attrs.gid = new_gid);
        new_attrs
            .atime
            .map(|new_atime| file_attrs.atime = new_atime);
        new_attrs
            .mtime
            .map(|new_mtime| file_attrs.mtime = new_mtime);
        new_attrs
            .crtime
            .map(|new_crtime| file_attrs.crtime = new_crtime);

        Ok(file_attrs)
    }

    /// Updates the extended attributes on an file.
    pub fn setxattr(&mut self, ino: u64, name: &OsStr, value: &[u8]) -> Result<(), Error> {
        let name = name.to_str().unwrap();
        let value = from_utf8(value).unwrap();
        debug!("setxattr(ino={}, name={}, value={})", ino, name, value);

        // let meta = fs::metadata(value).unwrap();

        self.conn.execute("INSERT INTO dependencies (inode, dep_inode, type) VALUES (?1, ?2, ?3)",
                          params![ino, value.parse::<u64>().unwrap(), name]).unwrap();

        // let memfile = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;
        // if memfile.xattr.contains_key(name) {
        //     *memfile.xattr.get_mut(name).unwrap() = String::from(value);
        // } else {
        //     memfile.xattr.insert(name.parse().unwrap(), String::from(value));
        // }
        Ok(())
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
        let entries_iter = stmt.query_map(params![ino, FileTypeToInt(FileType::Directory)], |row| {
            Ok((row.get::<_, InodeId>(0)?, IntToFileType(row.get(1).unwrap()), row.get::<_, String>(2)?))
        }).unwrap();

        for entry in entries_iter {
            entries.push(entry.unwrap());
        }
        debug!("Results: {:?}", entries);

        Ok(entries)

        // Rust version
        // self.inodes.get(&ino).map_or(Err(Error::NoEntry), |inode| {
        //     entries.push((inode.parent, FileType::Directory, String::from("..")));
        //
        //     for (child, child_ino) in inode.children.iter() {
        //         let child_attrs = &self.attrs.get(child_ino).unwrap();
        //         trace!("\t inode={}, child={}", child_ino, child);
        //         entries.push((child_attrs.ino, child_attrs.kind, String::from(child)));
        //     }
        //
        //     Ok(entries)
        // })
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
                atime: Timespec { sec: row.get(3)?, nsec: 0 },
                mtime: Timespec { sec: row.get(4)?, nsec: 0 },
                ctime: Timespec { sec: row.get(5)?, nsec: 0 },
                crtime: Timespec { sec: row.get(6)?, nsec: 0 },
                kind: IntToFileType(row.get(7)?),
                perm: row.get(8)?,
                nlink: row.get(9)?,
                uid: row.get(10)?,
                gid: row.get(11)?,
                rdev: row.get(12)?,
                flags: row.get(13)?,
            })
        }) {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::NoEntry)
        }

        // Rust version
        // let parent_inode = self.inodes.get(&parent).ok_or(Error::NoEntry)?;
        // let inode = parent_inode.children.get(name_str).ok_or(Error::NoEntry)?;
        // self.attrs.get(inode).ok_or(Error::NoEntry)
    }

    pub fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        debug!("rmdir(parent={}, name={})", parent, name_str);

        let parent_inode: &Inode = self.inodes.get(&parent).ok_or(Error::ParentNotFound)?;
        // let rmdir_inode_id: InodeId = *parent_inode.children.get(name_str).ok_or(Error::NoEntry)?;

        self.conn.execute("\
        UPDATE inodes SET nlink = nlink - 1 WHERE inode = (SELECT inode FROM tree WHERE parent = ?1 AND name = ?2);",
                          params![parent, name_str]).unwrap();
        self.conn.execute("\
        DELETE FROM tree WHERE parent = ?1 AND name = ?2;",
                          params![parent, name_str]).unwrap();
        self.conn.execute("DELETE FROM inodes WHERE nlink = 0", []).unwrap();

        Ok(())

        // let dir = self.inodes.get(&rmdir_inode_id).ok_or(Error::NoEntry)?;
        // if dir.children.is_empty() {
        //     self.attrs.remove(&rmdir_inode_id);
        //     let parent_inode: &mut Inode =
        //         self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
        //     parent_inode
        //         .children
        //         .remove(&name.to_str().unwrap().to_string());
        //     self.inodes.remove(&rmdir_inode_id);
        //     Ok(())
        // } else {
        //     Err(Error::NotEmpty)
        // }
    }

    pub fn mkdir(&mut self, parent: u64, name: &OsStr, _mode: u32) -> Result<&FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("mkdir(parent={}, name={})", parent, name_str);

        let new_inode_nr = self.get_next_ino();
        let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;

        let new_inode_nr: u64 = self.conn.query_row("SELECT max(inode) as max_index FROM inodes", [],
                                                    |max_index| Ok(max_index.get::<_, u64>(0)?)).unwrap() + 1;

        self.conn.execute("REPLACE INTO inodes (inode, atime, mtime, ctime, crtime, kind) VALUES (?3, ?2, ?2, ?2, ?2, ?1)",
                          params![FileTypeToInt(FileType::Directory), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), new_inode_nr]).unwrap();
        self.conn.execute("REPLACE INTO tree VALUES (?1, ?2, ?3)", params![new_inode_nr, parent, name_str.to_string()]).unwrap();

        if !parent_ino.children.contains_key(name_str) {
            let ts = time::now().to_timespec();
            let attr = FileAttr {
                ino: new_inode_nr,
                size: 0,
                blocks: 0,
                atime: ts,
                mtime: ts,
                ctime: ts,
                crtime: ts,
                kind: FileType::Directory,
                perm: 0o644,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            };

            parent_ino
                .children
                .insert(name_str.to_string(), new_inode_nr);
            self.attrs.insert(new_inode_nr, attr);
            self.inodes
                .insert(new_inode_nr, Inode::new(name_str.to_string(), parent));

            let stored_attr = self
                .attrs
                .get(&new_inode_nr)
                .expect("Shouldn't fail we just inserted it");
            Ok(stored_attr)
        } else {
            // A child with the given name already exists
            Err(Error::AlreadyExists)
        }
    }

    pub fn unlink(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        debug!("unlink(parent={}, name={})", parent, name_str);

        // let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
        // trace!("parent is {} for name={}", parent_ino.name, name_str);
        //
        // let old_ino = parent_ino
        //     .children
        //     .remove(&name_str.to_string())
        //     .ok_or(Error::NoEntry)?;
        //
        // let attr = self
        //     .attrs
        //     .remove(&old_ino)
        //     .expect("Inode needs to be in `attrs`.");
        //
        // if attr.kind == FileType::RegularFile {
        //     self.files
        //         .remove(&old_ino)
        //         .expect("Regular file inode needs to be in `files`.");
        // }
        //
        // self.inodes
        //     .remove(&old_ino)
        //     .expect("Child inode (to be unlinked) needs to in `inodes`.");

        self.conn.execute("\
        UPDATE inodes SET nlink = nlink - 1 WHERE inode = (SELECT inode FROM tree WHERE parent = ?1 AND name = ?2);",
                          params![parent, name_str]).unwrap();
        self.conn.execute("\
        DELETE FROM tree WHERE parent = ?1 AND name = ?2;",
                          params![parent, name_str]).unwrap();
        self.conn.execute("DELETE FROM inodes WHERE nlink = 0", []).unwrap();

        Ok(())
    }

    pub fn create(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<&FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!(
            "create(parent={}, name={}, mode={}, flags={})",
            parent, name_str, mode, flags,
        );

        let new_inode_nr = self.get_next_ino(); // TODO: should only generate it when we're sure it'll succeed.
        let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;


        // DB version
        // let mut stmt = self.conn.prepare("SELECT inode, name FROM tree WHERE parent = ?1").unwrap();
        // let children = stmt.query_map([parent], |row|  {
        //     Ok((row.get(0)?, row.get(1)?))
        // }).unwrap();

        let new_inode_nr: u64 = self.conn.query_row("SELECT max(inode) as max_index FROM inodes", [],
                                                    |max_index| Ok(max_index.get::<_, u64>(0)?)).unwrap() + 1;

        self.conn.execute("REPLACE INTO inodes (inode, atime, mtime, ctime, crtime, kind) VALUES (?3, ?2, ?2, ?2, ?2, ?1)",
                          params![FileTypeToInt(FileType::RegularFile), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), new_inode_nr]).unwrap();
        self.conn.execute("REPLACE INTO tree VALUES (?1, ?2, ?3)", params![new_inode_nr, parent, name_str.to_string()]).unwrap();

        match parent_ino.children.get_mut(&name_str.to_string()) {
            Some(child_ino) => {
                let attrs = self
                    .attrs
                    .get(&child_ino)
                    .expect("Existing child inode needs to be in `attrs`.");
                Ok(attrs)
            }
            None => {
                trace!(
                    "create file not found( parent={}, name={})",
                    parent,
                    name_str
                );

                let ts = time::now().to_timespec();
                self.attrs.insert(
                    new_inode_nr,
                    FileAttr {
                        ino: new_inode_nr,
                        size: 0,
                        blocks: 0,
                        atime: ts,
                        mtime: ts,
                        ctime: ts,
                        crtime: ts,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 0,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    },
                );
                self.files.insert(new_inode_nr, MemFile::new());
                parent_ino
                    .children
                    .insert(name_str.to_string(), new_inode_nr);
                self.inodes
                    .insert(new_inode_nr, Inode::new(name_str.to_string(), parent));

                let stored_attr = self
                    .attrs
                    .get(&new_inode_nr)
                    .expect("Shouldn't fail we just inserted it.");

                self.conn.execute("INSERT INTO files (ino, name) VALUES (?1, ?2)",
                                  params![new_inode_nr, name_str]);

                Ok(stored_attr)
            }
        }
    }

    pub fn write(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
    ) -> Result<u64, Error> {
        debug!("write(ino={}, fh={}, offset={})", ino, fh, offset);
        let ts = time::now().to_timespec();

        let fp = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;
        let attr = self
            .attrs
            .get_mut(&ino)
            .expect("Need to have attrs for file if it exists");

        let size = fp.update(data, offset);

        let size: u64 = self.conn.query_row("SELECT size FROM inodes WHERE inode = ?1", params![ino],
                                            |max_index| Ok(max_index.get::<_, u64>(0)?)).unwrap();

        let mut blob = self.conn.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false).unwrap();
        if size > (offset + data.len() as i64) as u64 {
            debug!("write within current size");
            blob.write_at(data, offset as usize);
        } else {
            debug!("write exceeds current size");
            let mut buf = Vec::new();
            buf.resize((offset + data.len() as i64) as usize, 0);
            blob.read_to_end(buf.as_mut());
            for n in 0..data.len() {
                buf[n + offset as usize] = data[n];
            }
            self.conn.execute("UPDATE inodes SET data = zeroblob(?2), size = ?2 WHERE inode = ?1", params![ino, offset + data.len() as i64]);
            blob.write_all(buf.as_slice());
        }

        self.conn.execute("UPDATE inodes SET atime = ?1, mtime = ?1 WHERE inode = ?2",
                          params![SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), ino]).unwrap();

        // Update file attributes
        attr.atime = ts;
        attr.mtime = ts;
        attr.size = fp.size();

        trace!(
            "write done(ino={}, wrote={}, offset={}, new size={})",
            ino,
            size,
            offset,
            fp.size()
        );

        Ok(size)
    }

    pub fn read(&mut self, ino: u64, fh: u64, offset: i64, size: u32) -> Result<Vec<u8>, Error> {
        debug!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino, fh, offset, size
        );

        // let fp = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;
        // let attr = self
        //     .attrs
        //     .get_mut(&ino)
        //     .expect("Need to have attrs for file if it exists");
        // attr.atime = time::now().to_timespec();
        //
        // Ok(&fp.bytes[offset as usize..])

        match self.conn.blob_open(DatabaseName::Main, "inodes", "data", ino as i64, false) {
            Ok(blob) => {
                let mut buf = Vec::new();
                buf.resize(size as usize, 0);
                blob.read_at(buf.as_mut(), offset as usize);
                Ok(buf)
            }
            Err(_) => {
                Err(Error::NoEntry)
            }
        }
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

        // let child_ino = {
        //     let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
        //     parent_ino
        //         .children
        //         .remove(&current_name_str.to_string())
        //         .ok_or(Error::ChildNotFound)?
        // };
        //
        // let new_parent_ino = self
        //     .inodes
        //     .get_mut(&new_parent)
        //     .ok_or(Error::NewParentNotFound)?;
        //
        // new_parent_ino
        //     .children
        //     .insert(new_name_str.to_string(), child_ino);

        self.conn.execute("UPDATE tree SET parent = ?1, name = ?2 WHERE parent = ?3 AND name = ?4",
                          params![new_parent, new_name_str, parent, current_name_str]);

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
                reply.attr(&TTL, &attr)
            }
            Err(e) => {
                error!("getattr reply with errno = {:?}", e);
                reply.error(e.into())
            }
        }
    }

    fn getxattr(
        &mut self,
        _req: &Request,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
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

    fn listxattr(
        &mut self,
        _req: &Request,
        ino: u64,
        size: u32,
        reply: ReplyXattr,
    ) {
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
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        fh: Option<u64>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        bkuptime: Option<Timespec>,
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
            fh,
            crtime,
            chgtime,
            bkuptime,
            flags,
        };

        let r = self.setattr(ino, new_attrs);
        match r {
            Ok(fattrs) => {
                reply.attr(&TTL, fattrs);
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
        _flags: u32,
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

    fn removexattr(
        &mut self,
        _req: &Request,
        ino: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) {
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
                    reply.add(entry.0, i as i64, entry.1, entry.2);
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        };
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup(parent, name) {
            Ok(attr) => {
                reply.entry(&TTL, &attr, 0);
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

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        match self.mkdir(parent, name, mode) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn open(&mut self, _req: &Request, _ino: u64, flags: u32, reply: ReplyOpen) {
        trace!("open(ino={}, _flags={})", _ino, flags);
        reply.opened(0, flags);
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
        flags: u32,
        reply: ReplyCreate,
    ) {
        match self.create(parent, name, mode, flags) {
            Ok(attr) => reply.created(&TTL, attr, 0, 0, 0),
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
        flags: u32,
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
        reply: ReplyEmpty,
    ) {
        match self.rename(parent, name, newparent, newname) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn memfs_update() {
        let mut f = MemFile::new();
        f.update(&[0, 1, 2, 3, 4, 5, 6, 7, 8], 0);
        assert_eq!(f.size(), 9);

        f.update(&[0, 0], 0);
        assert_eq!(f.size(), 9);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 8]);

        f.update(&[1, 1], 8);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 1, 1]);
        assert_eq!(f.size(), 10);

        f.update(&[2, 2], 13);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 1, 1, 0, 0, 0, 2, 2]);
        assert_eq!(f.size(), 15);
    }
}
