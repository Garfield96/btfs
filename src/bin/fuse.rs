use btfs::MemFilesystem;
use fuser::MountOption;

use clap::{App, Arg};

fn main() {
    env_logger::init();

    let args = App::new("SQLFS")
        .author("Christian Menges")
        .arg(
            Arg::with_name("MOUNTPOINT")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("DATADIR")
                .short("d")
                .long("datadir")
                .value_name("datadir")
                .takes_value(true)
                .required(cfg!(feature = "ext4")),
        )
        .get_matches();

    let mountpoint = args.value_of("MOUNTPOINT").unwrap();
    let mut fs = MemFilesystem::new();
    if let Some(data_dir) = args.value_of("DATADIR") {
        fs.set_data_dir(data_dir);
    }

    let options = [MountOption::AllowOther, MountOption::AutoUnmount];
    fuser::mount2(fs, &mountpoint, &options).unwrap();
}
