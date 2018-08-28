env = require('test_run')
test_run = env.new()

-- create master instance
test_run:cmd("create server trig_master with script='box/lua/trig_master.lua'")
test_run:cmd("start server trig_master")
test_run:cmd("switch trig_master")
env = require('test_run')
test_run = env.new()

-- simple ctl_event_case
ctl_const = box.ctl_event.const()
test_run:cmd("setopt delimiter ';'")
function on_replace(old, new)
    return box.tuple.new({new[1], new[2], 'M'})
end;
function on_ctl_trig(event)
    if event.type == ctl_const.SPACE and
       event.action == ctl_const.SPACE_CREATE then
        local space = box.space[event.space_id]
        space:before_replace(on_replace)
    end
end;
test_run:cmd("setopt delimiter ''");
active_trig = box.ctl_event.on_ctl_event(on_ctl_trig)
t1 = box.schema.space.create('trig1')
_ = t1:create_index('pk')
t1:replace({1, 2})
t1:select()

-- clear the trigger
box.ctl_event.on_ctl_event(nil, active_trig)
t2 = box.schema.space.create('trig2')
_ = t2:create_index('pk')
t2:replace({1, 2})
t2:select()

test_run:cmd("push filter 'replica: [-0-9a-f]+' to 'server: <master-uuid>'")
test_run:cmd("push filter 'space_id: [0-9]+' to 'space_id: <space_id>'")

-- create replica and test bootstrap events
box.schema.user.grant('guest', 'replication', nil, nil, {if_not_exists = true})
test_run:cmd("create server trig_replica with rpl_master=trig_master, script='box/lua/trig_replica.lua'")
test_run:cmd("start server trig_replica")
t1:replace({2, 3})
t1:select()

test_run:cmd("switch trig_replica")
env = require('test_run')
test_run = env.new()

-- list all events
events

-- check before replace trigger
box.space.trig1:select()

test_run:cmd("switch trig_master")
test_run:cmd("stop server trig_replica")

t1:replace({3, 4})

test_run:cmd("start server trig_replica")

test_run:cmd("switch trig_replica")
env = require('test_run')
test_run = env.new()

-- list all events
events

-- check tuples changed only one time
box.space.trig1:select()
test_run:cmd("switch default")
test_run:cmd("stop server trig_replica")
test_run:cmd("stop server trig_master")
