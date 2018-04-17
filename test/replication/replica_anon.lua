#!/usr/bin/env tarantool

box.cfg({
    listen              = os.getenv("LISTEN"),
    replication         = os.getenv("MASTER"),
    memtx_memory        = 107374182,
    read_only 		= true,
    replication_anon 	= true
})

require('console').listen(os.getenv('ADMIN'))
