#!/usr/bin/env tarantool
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen              = os.getenv("LISTEN"),
    memtx_memory        = 107374182,
    replication_connect_timeout = 0.5,
})
