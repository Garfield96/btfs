use btfs::MemFilesystem;
use log::error;

fn main() {
    env_logger::init();

    let fs = MemFilesystem::new();
    let mountpoint = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            error!("Usage: {} <MOUNTPOINT>.  Missing mountpoint argument", std::env::args().nth(0).unwrap());
            return;
        }
    };
    fuse::mount(fs, &mountpoint, &[]).unwrap();
}