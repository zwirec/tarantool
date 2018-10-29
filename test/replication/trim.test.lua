test_run = require('test_run').new()

SERVERS = {'trim1', 'trim2', 'trim3'}

-- Deploy cluster
test_run:create_cluster(SERVERS, "replication", {args="0.1"})
test_run:wait_fullmesh(SERVERS)

test_run:cmd('switch trim1')
len = box.space._cluster:len()

-- no errors
replicaset_list_inactive()
replica_prune()
replicaset_purge()

-- create zombies after restart all replicas
test_run:cmd('switch trim1')
fiber = require('fiber')
old_trim2 = test_run:get_param('trim2', 'id')
old_trim3 = test_run:get_param('trim3', 'id')

len = box.space._cluster:len()
test_run:cmd('switch default')
test_run:cmd('stop server trim2')
test_run:cmd('cleanup server trim2')
test_run:cmd('start server trim2')
test_run:cmd('stop server trim3')
test_run:cmd('cleanup server trim3')
test_run:cmd('start server trim3')
test_run:cmd('switch trim1')

replicaset_list_inactive() ~= nil
box.space._cluster:len() == len + #replicaset_list_inactive()

-- check that we showed and throw away only dead replicas
trim2 = test_run:get_param('trim2', 'id')
trim3 = test_run:get_param('trim3', 'id')

while box.info.replication[trim2[1]].upstream.status == 'follow' do fiber.sleep(0.01) end
while box.info.replication[trim3[1]].upstream.status =='follow' do fiber.sleep(0.01) end
box.info.replication[trim2[1]].downstream.status == nil
box.info.replication[trim3[1]].downstream.status == nil

box.info.replication[old_trim2[1]].upstream == nil
box.info.replication[old_trim3[1]].upstream == nil
box.info.replication[old_trim2[1]].downstream.status == 'stopped'
box.info.replication[old_trim3[1]].downstream.status == 'stopped'
--
replicaset_list_inactive() == 2
replicaset_purge(replicaset_list_inactive())
#replicaset_list_inactive() == 0
box.space._cluster:len() == len

box.info.replication[trim2[1]] ~= nil
box.info.replication[trim3[1]] ~= nil
box.info.replication[trim2[1]].downstream.status == nil
box.info.replication[trim3[1]].downstream.status == nil

box.info.replication[old_trim2[1]] == nil
box.info.replication[old_trim3[1]] == nil


-- no applier no relay

test_run:cmd('switch default')
test_run:cmd('stop server trim2')
test_run:cmd('cleanup server trim2')
test_run:cmd('start server trim2')
test_run:cmd('stop server trim3')
test_run:cmd('cleanup server trim3')
test_run:cmd('start server trim3')
test_run:cmd('stop server trim1')
test_run:cmd('cleanup server trim1')
test_run:cmd('start server trim1')
test_run:cmd('switch trim1')

box.ctl.wait_rw(10)

inactive = replicaset_list_inactive()

-- prune given replica
replica_prune(inactive[1])
#replicaset_list_inactive() ~= #inactive
replicaset_purge(replicaset_list_inactive())
box.space._cluster:len() == 3

-- Cleanup
test_run:cmd('switch default')
test_run:drop_cluster(SERVERS)
