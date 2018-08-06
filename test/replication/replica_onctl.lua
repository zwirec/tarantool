#!/usr/bin/env tarantool

SYSTEM_SPACE_RECOVERY = 0
LOCAL_RECOVERY = 0
READ_ONLY = 0
READ_WRITE = 0
REPLICASET_ADD = {}
REPLICASET_REMOVE = {}

local function onctl(ctx)
    if ctx.type == box.ctl.event.SYSTEM_SPACE_RECOVERY then
        SYSTEM_SPACE_RECOVERY = SYSTEM_SPACE_RECOVERY + 1
    elseif ctx.type == box.ctl.event.LOCAL_RECOVERY then
        LOCAL_RECOVERY = LOCAL_RECOVERY + 1
    elseif ctx.type == box.ctl.event.READ_ONLY then
        READ_ONLY = READ_ONLY + 1
    elseif ctx.type == box.ctl.event.READ_WRITE then
        READ_WRITE = READ_WRITE + 1
    elseif ctx.type == box.ctl.event.REPLICASET_ADD then
        table.insert(REPLICASET_ADD, ctx.replica_id)
    elseif ctx.type == box.ctl.event.REPLICASET_REMOVE then
        table.insert(REPLICASET_REMOVE, ctx.replica_id)
    end
end

box.cfg({
    listen              = os.getenv("LISTEN"),
    replication         = os.getenv("MASTER"),
    memtx_memory        = 107374182,
    replication_connect_timeout = 0.5,
    on_ctl_event        = onctl,
})

require('console').listen(os.getenv('ADMIN'))
