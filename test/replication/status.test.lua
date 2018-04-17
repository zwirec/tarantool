env = require('test_run')
test_run = env.new()
test_run:cmd('restart server default with cleanup=1')
test_run:cmd('switch default')

--
-- No replication
--

master_id = box.info.id

#box.info.vclock == 0
#box.info.replication == 1
box.space._cluster:count() == 1

box.info.uuid == box.space._cluster:get(master_id)[2]
-- LSN is nil until a first request is made
box.info.vclock[master_id] == nil
--- box.info.lsn == box.info.vclock[master_id]
box.info.lsn == 0
-- Make the first request
box.schema.user.grant('guest', 'replication')
-- LSN is 1 after the first request
#box.info.vclock == 1
box.info.vclock[master_id] == 1
box.info.lsn == box.info.vclock[master_id]
master = box.info.replication[master_id]
master.id == master_id
master.uuid == box.space._cluster:get(master_id)[2]
master.lsn == box.info.vclock[master_id]
master.upstream == nil
master.downstream == nil

-- Start Master -> Slave replication
test_run:cmd("create server replica with rpl_master=default, script='replication/replica.lua'")
test_run:cmd("create server replica_anon with rpl_master=default, script='replication/replica_anon.lua'")
test_run:cmd("start server replica")

--
-- Master
--
test_run:cmd('switch default')

#box.info.vclock == 1 -- box.info.vclock[replica_id] is nil
#box.info.replication == 2
box.space._cluster:count() == 2

-- master's status
master_id = box.info.id
box.info.vclock[master_id] == 2 -- grant + registration == 2
box.info.lsn == box.info.vclock[master_id]
master = box.info.replication[master_id]
master.id == master_id
master.uuid == box.space._cluster:get(master_id)[2]
master.lsn == box.info.vclock[master_id]
master.upstream == nil
master.downstream == nil
master.anonymous == false

-- replica's status
replica_id = test_run:get_server_id('replica')
box.info.vclock[replica_id] == nil
replica = box.info.replication[replica_id]
replica.id == replica_id
replica.uuid == box.space._cluster:get(replica_id)[2]
-- replica.lsn == box.info.vclock[replica_id]
replica.lsn == 0
replica.upstream == nil
replica.downstream.vclock[master_id] == box.info.vclock[master_id]
replica.downstream.vclock[replica_id] == box.info.vclock[replica_id]

--
-- Replica
--
test_run:cmd('switch replica')

#box.info.vclock == 1 -- box.info.vclock[replica_id] is nil
#box.info.replication == 2
box.space._cluster:count() == 2

-- master's status
master_id = test_run:get_server_id('default')
box.info.vclock[master_id] == 2
master = box.info.replication[master_id]
master.id == master_id
master.uuid == box.space._cluster:get(master_id)[2]
master.upstream.status == "follow"
master.upstream.lag < 1
master.upstream.idle < 1
master.upstream.peer:match("localhost")
master.downstream == nil
master.anonymous == false

-- replica's status
replica_id = box.info.id
box.info.vclock[replica_id] == nil
-- box.info.lsn == box.info.vclock[replica_id]
box.info.lsn == 0
replica = box.info.replication[replica_id]
replica.id == replica_id
replica.uuid == box.space._cluster:get(replica_id)[2]
-- replica.lsn == box.info.vclock[replica_id]
replica.lsn == 0
replica.upstream == nil
replica.downstream == nil
replica.anonymous == false

test_run:cmd('switch default')
-- Start anonymous replica
test_run:cmd("start server replica_anon")
anon_init_id = 1073741823
#box.info.vclock == 1 -- box.info.vclock[replica_id] is nil
box.info.replication[anon_init_id] ~= nil
box.space._cluster:count() == 2

-- master's status
box.info.vclock[master_id] == 2 -- grant + registration == 2
box.info.lsn == box.info.vclock[master_id]
master = box.info.replication[master_id]
master.id == master_id
master.uuid == box.space._cluster:get(master_id)[2]
master.lsn == box.info.vclock[master_id]
master.upstream == nil
master.downstream == nil
master.anonymous == false

-- anon replica's status

box.info.vclock[anon_init_id] == nil
replica = box.info.replication[anon_init_id]
replica.id == anon_init_id
box.space._cluster:get(anon_init_id)
-- replica.lsn == box.info.vclock[anon_init_id]
replica.lsn == 0
replica.upstream == nil
replica.downstream.vclock[master_id] == box.info.vclock[master_id]
replica.downstream.vclock[anon_init_id] == box.info.vclock[anon_init_id]
replica.anonymous == true

test_run:cmd('switch replica_anon')
#box.info.vclock == 1 -- box.info.vclock[replica_id] is nil
#box.info.replication == 2
box.space._cluster:count() == 2

-- master's status
master_id = test_run:get_server_id('default')
box.info.vclock[master_id] == 2
master = box.info.replication[master_id]
master.id == master_id
master.uuid == box.space._cluster:get(master_id)[2]
master.upstream.status == "follow"
master.upstream.lag < 1
master.upstream.idle < 1
master.upstream.peer:match("localhost")
master.downstream == nil
master.anonymous == false

replica_id = box.info.id
box.info.vclock[replica_id] == nil
box.info.lsn == 0
-- there is no information about itself on anon replica
box.info.replication[replica_id] == nil

--
-- ClientError during replication
--
test_run:cmd('switch replica')
box.space._schema:insert({'dup'})
test_run:cmd('switch default')
box.space._schema:insert({'dup'})
test_run:cmd('switch replica')
r = box.info.replication[1]
r.upstream.status == "stopped"
r.upstream.message:match('Duplicate') ~= nil
test_run:cmd('switch default')
box.space._schema:delete({'dup'})
test_run:cmd("push filter ', lsn: [0-9]+' to ', lsn: <number>'")
test_run:grep_log('replica', 'error applying row: .*')
test_run:cmd("clear filter")

--
-- Check box.info.replication login
--
test_run:cmd('switch replica')
test_run:cmd("set variable master_port to 'replica.master'")
replica_uri = os.getenv("LISTEN")
box.cfg{replication = {"guest@localhost:" .. master_port, replica_uri}}

master_id = test_run:get_server_id('default')
master = box.info.replication[master_id]
master.id == master_id
master.upstream.status == "follow"
master.upstream.peer:match("guest")
master.upstream.peer:match("localhost")
master.downstream == nil

test_run:cmd('switch default')
-- check subsribe from anonymous after master restart
test_run:cmd('switch default')
test_run:cmd("stop server replica_anon")
test_run:cmd("restart server default")
test_run:cmd("start server replica_anon")
anon_init_id = 1073741823
box.info.replication[anon_init_id] ~= nil

--
-- Cleanup
--
box.schema.user.revoke('guest', 'replication')
test_run:cmd("stop server replica")
test_run:cmd("stop server replica_anon")
test_run:cmd("cleanup server replica")
test_run:cmd("cleanup server replica_anon")
