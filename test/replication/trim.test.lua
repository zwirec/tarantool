test_run = require('test_run').new()

SERVERS = {'trim1', 'trim2', 'trim3', 'trim4'}


-- Deploy cluster
test_run:create_cluster(SERVERS, "replication", {args="0.1"})
test_run:wait_fullmesh(SERVERS)

test_run:cmd('switch trim1')
box.space._cluster:len() == 4
-- errors
replicaset_list_wasted()
replica_displace()

-- set dead/rw gap
box.cfg{replication_dead_gap = 0.001, replication_rw_gap = 10}

-- stop replication
test_run:cmd('switch trim4')
replication = box.cfg.replication
box.cfg{replication = {}}

test_run:cmd('switch trim1')
-- must be empty
table.getn(replicaset_list_wasted()) == 0
-- need time to fulfill dead_gap
wait()
wasted_replica = replicaset_list_wasted()
table.getn(wasted_replica) == 1

-- found by law
find_wasted_by_law(wasted_replica[1])
find_wasted_by_rw(wasted_replica[1])
find_wasted_by_lar(wasted_replica[1])

--turn on replication and see empty wasted list
test_run:cmd('switch trim4')
box.cfg{replication = replication}
test_run:cmd('switch trim1')
table.getn(replicaset_list_wasted()) == 0

-- look at rw_gap
box.cfg{replication_dead_gap = 10, replication_rw_gap = 0.001}
test_run:cmd('switch trim4')
box.cfg{replication = {}}
test_run:cmd('switch trim1')
wait()
table.getn(replicaset_list_wasted()) == 1

find_wasted_by_rw(wasted_replica[1])
find_wasted_by_law(wasted_replica[1])
find_wasted_by_lar(wasted_replica[1])

-- look at lar
test_run:cmd('switch trim4')
box.cfg{replication = replication}
test_run:cmd('switch trim1')
table.getn(replicaset_list_wasted()) == 0
box.cfg{replication_dead_gap = 0.001, replication_rw_gap = 10}
test_run:cmd('stop server trim4')
table.getn(replicaset_list_wasted()) == 0
wait()
wasted_replica = replicaset_list_wasted()
table.getn(wasted_replica)  == 1

find_wasted_by_lar(wasted_replica[1])
find_wasted_by_rw(wasted_replica[1])
find_wasted_by_law(wasted_replica[1])

-- throw away dead replicas
-- delete given replica
box.space._cluster:len() == 4
replica_displace(wasted_replica[1])
box.space._cluster:len() == 3

-- trim replicaset
test_run:cmd('stop server trim2')
test_run:cmd('stop server trim3')

wait()
trim_set = replicaset_list_wasted()
table.getn(trim_set) == 2
replicaset_trim(trim_set)
box.space._cluster:len() == 1

-- Cleanup
test_run:cmd('start server trim2')
test_run:cmd('start server trim3')
test_run:cmd('start server trim4')

test_run:cmd('switch default')
test_run:drop_cluster(SERVERS)
