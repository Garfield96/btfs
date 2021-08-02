# SQLFS

A basic simple filesystem using a database as backend store.

## Build Docker Image
```sh
docker build -t sqlfs docker/
```
## Run Docker
```sh
docker run --device /dev/fuse \
           --cap-add SYS_ADMIN \
           --security-opt apparmor:unconfined \
           --rm -it -v $(pwd):/sqlfs sqlfs
```
In case this command does not work, use `--privileged` flag (see also [https://stackoverflow.com/a/49021109](https://stackoverflow.com/a/49021109))

## Dependencies

### SQLite

```
$ apt-get install sqlite3 sqlite3-pcre
```

### FUSE 
Set up FUSE:
```
$ apt-get install fuse libfuse-dev

# Make sure fuse.conf has allow_root and user_allow_other set:
$ cat /etc/fuse.conf
allow_root
user_allow_other
```
## Run
Test the system:
```sh
mkdir /tmp/test
RUST_LOG=trace cargo run --bin fuse /tmp/test
```

Unmount:
```sh
$ umount /tmp/test
```

## Testing with filebench

Installing filebench ([https://github.com/filebench/filebench](https://github.com/filebench/filebench)):

```sh
sudo apt-get install bison flex
git clone https://github.com/filebench/filebench.git
libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure
make
```

Running a benchmark:
```sh
mkdir -p /tmp/fbtest
cargo run --release --bin fuse -- /tmp/fbtest/ &

sudo sh -c "echo 0 > /proc/sys/kernel/randomize_va_space"
./filebench -f randomrw.f

umount /tmp/fbtest
```
