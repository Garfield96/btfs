FROM ubuntu:latest

RUN apt-get -y update && apt-get -y upgrade
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl libssl-dev ca-certificates build-essential sqlite3 libsqlite3-dev sqlite3-pcre postgresql \
    fuse3 libfuse3-dev pkg-config tmux
USER postgres
RUN service postgresql start && \
    psql -c "ALTER USER postgres WITH PASSWORD 'postgres'" && \
    psql -c "CREATE DATABASE shell";
USER root
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENTRYPOINT /fastshell/docker/start.sh && /bin/bash