env = require('test_run')
test_run = env.new()

test_run:cmd("create server master with script='replication/master_onctl.lua'")
test_run:cmd("create server replica with rpl_master=master, script='replication/replica_onctl.lua'")

test_run:cmd("start server master")
test_run:cmd("switch master")
box.schema.user.grant('guest', 'replication')

SYSTEM_SPACE_RECOVERY
LOCAL_RECOVERY
READ_ONLY
READ_WRITE
-- must be two entries. First from bootstrap.snap, second for current instance.
REPLICASET_ADD
-- must be one entry. Deletion of initial tuple in _cluster space.
REPLICASET_REMOVE
REPLICA_CONNECTION_ERROR

REPLICASET_ADD = {}
REPLICASET_REMOVE = {}

new_replica_id = 0
deleted_replica_id = 0

test_run:cmd("setopt delimiter ';'")
function on_ctl_new(ctx)
    if ctx.type == box.ctl.event.REPLICASET_ADD then
        new_replica_id = ctx.replica_id
    elseif ctx.type == box.ctl.event.REPLICASET_REMOVE then
        deleted_replica_id = ctx.replica_id
    end
end;
test_run:cmd("setopt delimiter ''");

_ = box.ctl.on_ctl_event(on_ctl_new)

test_run:cmd("start server replica")

REPLICASET_ADD
REPLICASET_REMOVE
REPLICA_CONNECTION_ERROR

new_replica_id
deleted_replica_id

test_run:cmd("switch replica")

test_run:cmd("setopt delimiter ';'")
function on_ctl_shutdown(ctx)
    if ctx.type == box.ctl.event.SHUTDOWN then
        require("log").info("test replica shutdown")
    end
end;

function on_ctl_error(ctx)
    error("trigger error")
end;

test_run:cmd("setopt delimiter ''");

SYSTEM_SPACE_RECOVERY
LOCAL_RECOVERY
READ_ONLY
READ_WRITE
REPLICASET_ADD
REPLICASET_REMOVE

box.cfg{read_only = true}
fiber = require("fiber")
while READ_ONLY == 0 do fiber.sleep(0.001) end
READ_ONLY

box.cfg{on_ctl_event = on_ctl_error}
box.cfg{read_only = false}
test_run:grep_log('replica', 'ctl_trigger error')
box.cfg{on_ctl_event = on_ctl_shutdown}

test_run:cmd("restart server replica")
-- TODO: test SHUTDOWN, wait for pull request on grep_log to grep logs of killed replica.
-- test_run:grep_log('replica', 'test replica shutdown', 10000, true)

test_run:cmd("switch master")
REPLICA_CONNECTION_ERROR

box.schema.user.revoke('guest', 'replication')
_ = box.space._cluster:delete{2}

SYSTEM_SPACE_RECOVERY
LOCAL_RECOVERY
READ_ONLY
READ_WRITE
REPLICASET_ADD
REPLICASET_REMOVE

new_replica_id
deleted_replica_id

box.ctl.on_ctl_event(nil, on_ctl_new)

-- cleanup
test_run:cmd("switch default")
test_run:cmd("stop server master")
test_run:cmd("cleanup server master")
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")
