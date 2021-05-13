use btfs::MemFilesystem;
use log::error;

fn main() {
    env_logger::init();

    let mountpoint = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            error!(
                "Usage: {} <MOUNTPOINT>.  Missing mountpoint argument",
                std::env::args().nth(0).unwrap()
            );
            return;
        }
    };
    let fs = MemFilesystem::new();
    fuse::mount(fs, &mountpoint, &[]).unwrap();
}
