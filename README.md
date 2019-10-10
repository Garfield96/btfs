# btfs

A basic simple in-memory file-system using b-trees.

## FUSE Example

Set up FUSE:
```
$ apt-get install fuse libfuse-dev

# Make sure fuse.conf has allow_root and user_allow_other set:
$ cat /etc/fuse.conf
allow_root
user_allow_other
```

Test the system:
```
$ RUST_LOG=trace cargo run --bin fuse /tmp/test
```

Unmount:
```
$ umount /tmp/test
```

## Testing with filebench

Installing filebench:

```
$ git clone https://github.com/filebench/filebench.git
$ libtoolize
$ aclocal
$ autoheader
$ automake --add-missing
$ autoconf
$ ./configure
$ make
```

Running a benchmark:
```
$ mkdir -p /tmp/fbtest
$ cargo run --release --bin fuse -- /tmp/fbtest/ &

$ echo 0 > /proc/sys/kernel/randomize_va_space
$ ./filebench -f randomrw.f

$ umount /tmp/fbtest
```