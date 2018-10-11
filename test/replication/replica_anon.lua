#!/usr/bin/env tarantool

require("console").listen(os.getenv("ADMIN"))

box.cfg{
    read_only=true,
    replica_anon=true,
    replication=os.getenv("MASTER")
}
