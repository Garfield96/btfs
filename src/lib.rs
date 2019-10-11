//! A simple implementation of an in-memory files-sytem written in Rust using the BTreeMap
//! data-structure.
//!
//! This code is inspired from https://github.com/bparli/bpfs and was modified to work
//! with node-replication for benchmarking.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::iter;

use fuse::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use libc::{c_int, EEXIST, EINVAL, ENOENT, ENOTEMPTY};
use time::Timespec;

use log::{debug, error, info, trace};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

pub type InodeId = u64;

// Re-export reused structs from fuse:
pub use fuse::FileAttr;
pub use fuse::FileType;

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
}

impl MemFile {
    pub fn new() -> MemFile {
        MemFile { bytes: Vec::new() }
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
}

impl MemFilesystem {
    pub fn new() -> MemFilesystem {
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
        MemFilesystem {
            files: files,
            attrs: attrs,
            inodes: inodes,
            next_inode: 2,
        }
    }

    /// Generates inode numbers.
    fn get_next_ino(&mut self) -> u64 {
        self.next_inode += 1;
        self.next_inode
    }

    pub fn getattr(&mut self, ino: u64) -> Result<&FileAttr, Error> {
        debug!("getattr(ino={})", ino);
        self.attrs.get(&ino).ok_or(Error::NoEntry)
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

    pub fn readdir(
        &mut self,
        ino: InodeId,
        _fh: u64,
    ) -> Result<Vec<(InodeId, FileType, String)>, Error> {
        debug!("readdir(ino={}, fh={})", ino, _fh);
        let mut entries: Vec<(u64, FileType, String)> = Vec::with_capacity(32);
        entries.push((ino, FileType::Directory, String::from(".")));

        self.inodes.get(&ino).map_or(Err(Error::NoEntry), |inode| {
            entries.push((inode.parent, FileType::Directory, String::from("..")));

            for (child, child_ino) in inode.children.iter() {
                let child_attrs = &self.attrs.get(child_ino).unwrap();
                trace!("\t inode={}, child={}", child_ino, child);
                entries.push((child_attrs.ino, child_attrs.kind, String::from(child)));
            }

            Ok(entries)
        })
    }

    pub fn lookup(&mut self, parent: u64, name: &OsStr) -> Result<&FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("lookup(parent={}, name={})", parent, name_str);

        let parent_inode = self.inodes.get(&parent).ok_or(Error::NoEntry)?;
        let inode = parent_inode.children.get(name_str).ok_or(Error::NoEntry)?;
        self.attrs.get(inode).ok_or(Error::NoEntry)
    }

    pub fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        let name_str = name.to_str().unwrap();
        debug!("rmdir(parent={}, name={})", parent, name_str);

        let parent_inode: &Inode = self.inodes.get(&parent).ok_or(Error::ParentNotFound)?;
        let rmdir_inode_id: InodeId = *parent_inode.children.get(name_str).ok_or(Error::NoEntry)?;

        let dir = self.inodes.get(&rmdir_inode_id).ok_or(Error::NoEntry)?;
        if dir.children.is_empty() {
            self.attrs.remove(&rmdir_inode_id);
            let parent_inode: &mut Inode =
                self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
            parent_inode
                .children
                .remove(&name.to_str().unwrap().to_string());
            self.inodes.remove(&rmdir_inode_id);
            Ok(())
        } else {
            Err(Error::NotEmpty)
        }
    }

    pub fn mkdir(&mut self, parent: u64, name: &OsStr, _mode: u32) -> Result<&FileAttr, Error> {
        let name_str = name.to_str().unwrap();
        debug!("mkdir(parent={}, name={})", parent, name_str);

        let new_inode_nr = self.get_next_ino();
        let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
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

        let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
        trace!("parent is {} for name={}", parent_ino.name, name_str);

        let old_ino = parent_ino
            .children
            .remove(&name_str.to_string())
            .ok_or(Error::NoEntry)?;

        let attr = self
            .attrs
            .remove(&old_ino)
            .expect("Inode needs to be in `attrs`.");

        if attr.kind == FileType::RegularFile {
            self.files
                .remove(&old_ino)
                .expect("Regular file inode needs to be in `files`.");
        }

        self.inodes
            .remove(&old_ino)
            .expect("Child inode (to be unlinked) needs to in `inodes`.");
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

    pub fn read(&mut self, ino: u64, fh: u64, offset: i64, size: u32) -> Result<&[u8], Error> {
        debug!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino, fh, offset, size
        );

        let fp = self.files.get_mut(&ino).ok_or(Error::NoEntry)?;
        let attr = self
            .attrs
            .get_mut(&ino)
            .expect("Need to have attrs for file if it exists");
        attr.atime = time::now().to_timespec();

        Ok(&fp.bytes[offset as usize..])
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

        let child_ino = {
            let parent_ino = self.inodes.get_mut(&parent).ok_or(Error::ParentNotFound)?;
            parent_ino
                .children
                .remove(&current_name_str.to_string())
                .ok_or(Error::ChildNotFound)?
        };

        let new_parent_ino = self
            .inodes
            .get_mut(&new_parent)
            .ok_or(Error::NewParentNotFound)?;

        new_parent_ino
            .children
            .insert(new_name_str.to_string(), child_ino);

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
                reply.attr(&TTL, attr)
            }
            Err(e) => {
                error!("getattr reply with errno = {:?}", e);
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
                reply.entry(&TTL, attr, 0);
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
        reply.opened(0, 0);
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
            Ok(slice) => reply.data(slice),
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
