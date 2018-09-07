test_run = require('test_run').new()
test_run:create_cluster(CLUSTER, 'promote')
test_run:wait_fullmesh(CLUSTER)
--
-- Check the promote actually allows to switch the master.
--
_ = test_run:switch('box1')
-- Box1 read_only is auto.
box.cfg.read_only
-- And it is a master.
promote_info()

_ = test_run:switch('box2')
box.cfg.read_only
-- Box2 is a slave.
promote_info()
-- And can not do DDL/DML.
box.schema.create_space('test') -- Fail.

box.ctl.promote()
-- Now the slave has become a master.
promote_info()
-- And can do DDL/DML.
s = box.schema.create_space('test')
s:drop()

_ = test_run:switch('box1')
-- In turn, the old master is a slave now.
promote_info()
-- For him any DDL/DML is forbidden.
box.schema.create_space('test2')

-- Check a watcher state.
_ = test_run:switch('box3')
box.cfg.read_only
promote_info()

--
-- Try simple errors.
--
_ = test_run:switch('box2')

-- Invalid UUIDs.
prom:insert{1, 'invalid', 1, box.info.uuid, 1, 't'}
prom:insert{1, box.info.uuid, 1, 'invalid', 1, 't'}
-- Invalid ts.
prom:insert{1, box.info.uuid, 1, box.info.uuid, -1, 't'}
-- Invalid type.
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'invalid'}
-- Invalid type-specific options.
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'begin', {quorum = 1}}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'begin', {quorum = 'invalid', timeout = 1}}

map = setmetatable({}, {__serialize = 'map'})
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'status', {is_master = 'invalid'}}

prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'error', map}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'error', {code = 'code', message = 'msg'}}

prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'sync', map}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'success', map}

prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'begin', {quorum = 4, round_type = 'invalid'}}

--
-- Test simple invalid scenarios.
--

-- Already master.
box.ctl.promote()
_ = test_run:switch('box3')
-- Small quorum.
box.ctl.promote({quorum = 2})

--
-- Test timeout.
--
promote_check_error({timeout = 0.00001})
promote_info(error_safe_info)
_ = test_run:switch('box2')
promote_info(error_safe_info)
_ = test_run:switch('box1')
promote_info(error_safe_info)

--
-- Test recovery after failed promotion.
--
_ = test_run:switch('default')
_ = test_run:cmd('restart server box1')
_ = test_run:cmd('restart server box2')
_ = test_run:cmd('restart server box3')
_ = test_run:switch('box2')
promote_info(error_safe_info)
box.info.ro
_ = test_run:switch('box1')
promote_info(error_safe_info)
box.info.ro
_ = test_run:switch('box3')
promote_info(error_safe_info)
box.info.ro

--
-- Test basic demotion.
--
_ = test_run:switch('box1')
box.ctl.demote() -- Error - already demoted.
_ = test_run:switch('box2')
box.ctl.demote()
-- Still is master but demoted.
promote_info()
box.info.ro
_ = test_run:switch('box1')
promote_info()
box.info.ro
_ = test_run:switch('box3')
promote_info()
box.info.ro
-- Demotion is persisted.
_ = test_run:cmd('restart server box2')
_ = test_run:switch('box2')
promote_info()
box.info.ro

--
-- Test promotion in a completely read-only cluster.
--
_ = test_run:switch('box1')
box.info.ro
box.ctl.promote()
promote_info()
box.info.ro
_ = test_run:switch('box2')
promote_info()
box.info.ro
_ = test_run:switch('box3')
promote_info()
box.info.ro

--
-- Promote off and on.
--
_ = test_run:switch('box1')
box.ctl.demote()
box.ctl.promote()
promote_info()
box.info.ro

--
-- Fail to collect a quorum.
--
_ = test_run:switch('box2')
_ = test_run:cmd('stop server box1')
box.ctl.promote({timeout = 0.2})
promote_info()
box.info.ro
_ = test_run:switch('box3')
promote_info()
box.info.ro
_ = test_run:cmd('start server box1')
_ = test_run:switch('box1')
promote_info(error_safe_info)
box.info.ro

--
-- Work with a quorum != 100% and some of nodes down.
--
_ = test_run:cmd('stop server box3')
_ = test_run:switch('box2')
box.ctl.promote({quorum = 3})
promote_info()
box.info.ro
_ = test_run:switch('box1')
promote_info()
box.info.ro
_ = test_run:cmd('start server box3')
_ = test_run:switch('box3')
promote_info()
box.info.ro

--
-- Allow demote with small quorum.
--
_ = test_run:switch('box2')
_ = test_run:cmd('stop server box1')
_ = test_run:cmd('stop server box3')
_ = test_run:cmd('stop server box4')
box.ctl.demote({quorum = 0})
promote_info()
box.info.ro
_ = test_run:cmd('start server box1')
_ = test_run:cmd('start server box3')
_ = test_run:cmd('start server box4')
_ = test_run:switch('box1')
promote_info()
_ = test_run:switch('box3')
promote_info()
_ = test_run:switch('box4')
promote_info()

--
-- Test that promotion subsystem cleans _promotion space up. But
-- not after demotion. Each round for 4 participants produces <=
-- 10 messages (begin, 3*status, sync, 3*success, demote,
-- promote), so the total is <= 20.
--
count = prom:count()
count <= 20 and count > 10

--
-- After promotion reset the state is cleaned up.
--
_ = test_run:switch('box2')
box.ctl.promote()
box.info.ro
-- Error - promotion history is not empty.
box.cfg{read_only = true}
box.ctl.promote_reset()
box.info.ro
box.cfg{read_only = true}
box.info.ro
box.cfg{read_only = box.NULL}
-- The instance should be read-only despite it was a master
-- before promotion reset.
box.info.ro

--
-- Allow to make a usual cluster be promotable.
--
_ = test_run:switch('box1')
box.internal.initial_promote()
promote_info()
box.info.ro

_ = test_run:switch('default')
test_run:drop_cluster(CLUSTER)
