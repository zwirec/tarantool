test_run = require('test_run').new()

master_min_uuid = '42673fad-8386-4934-b89c-f90509148b9e'
master_max_uuid = 'a61c8ead-622f-49c0-8026-d77f9416895a'
replica_uuid = '3814ab36-a550-4aaf-846a-d80e3b7c4bcf'
SERVERS = {"master1", "master2"}

test_run:cmd('create server master1 with script="replication/mapping_master1.lua"')
test_run:cmd('create server master2 with script="replication/mapping_master2.lua"')
test_run:cmd(string.format('start server master1 with args="%s", wait_load=False, wait=False', master_min_uuid))
test_run:cmd(string.format('start server master2 with args="%s", wait_load=False, wait=False', master_max_uuid))

test_run:cmd('create server replica with script="replication/mapping_replica.lua"')
-- The scenario of test is following.
-- Launch two master servers(master-master pair). Switch off one of them and delete the one from _cluster
-- table on the remaining. Launch replica-slave of the remaining. Launch the switched server back.
-- Check replica id and xlog headers that they are mapped according to their _cluster tables.
--

test_run:wait_fullmesh(SERVERS)

test_run:cmd("switch master1")
box.space._cluster:select{}

test_run:cmd("switch master2")
box.space._cluster:select{}

test_run:cmd("switch default")
test_run:cmd("stop server master2")

test_run:cmd("switch master1")
box.space._cluster.index.replica_id:delete{2}

test_run:cmd("switch default")
test_run:cmd(string.format('start server replica with args="%s", wait_load=False, wait=False', replica_uuid))

test_run:cmd("switch master1")
box.space._cluster:select{}

test_run:cmd("switch replica")
box.space._cluster:select{}

test_run:cmd("switch default")
test_run:cmd(string.format('start server master2 with args="%s", wait_load=False, wait=False', master_max_uuid))
-- can't use wait_fullmesh here because replica id on different nodes differs.
test_run:cmd("switch master1")
fiber = require("fiber")
while box.info.replication[4] == nil or box.info.replication[4].upstream.status ~= "follow" do fiber.sleep(0.001) end

test_run:cmd("switch master2")
box.space._cluster:select{}
box.space._schema:insert{"key1", 1}
xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id

row.HEADER.replica_id -- must be 2

test_run:cmd("switch master1")
box.space._cluster:select{}
box.space._schema:get{"key1"}

xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id
row.HEADER.replica_id -- must be 4


test_run:cmd("switch replica")
box.space._cluster:select{}
box.space._schema:get{"key1"}

xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id
row.HEADER.replica_id -- must be 3


-- cleanup
test_run:cmd("switch default")
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")

test_run:drop_cluster(SERVERS)


test_run:cmd(string.format('start server master1 with args="%s", wait_load=False, wait=False', master_max_uuid))
test_run:cmd(string.format('start server master2 with args="%s", wait_load=False, wait=False', master_min_uuid))

-- The second part of the test.
-- The scenario is the same, but now the switched off server is
-- the main in master-master pair (the one with lowest uuid).

test_run:wait_fullmesh(SERVERS)

test_run:cmd("switch master1")
box.space._cluster:select{}

test_run:cmd("switch master2")
box.space._cluster:select{}

test_run:cmd("switch default")
test_run:cmd("stop server master2")

test_run:cmd("switch master1")
box.space._cluster.index.replica_id:delete{1}

test_run:cmd("switch default")
test_run:cmd(string.format('start server replica with args="%s", wait_load=False, wait=False', replica_uuid))

test_run:cmd("switch master1")
box.space._cluster:select{}

test_run:cmd("switch replica")
box.space._cluster:select{}

test_run:cmd("switch default")
test_run:cmd(string.format('start server master2 with args="%s", wait_load=False, wait=False', master_min_uuid))
-- can't use wait_fullmesh here because replica id on different nodes differs.
test_run:cmd("switch master1")
fiber = require("fiber")
while box.info.replication[4] == nil or box.info.replication[4].upstream.status ~= "follow" do fiber.sleep(0.001) end

test_run:cmd("switch master2")
box.space._cluster:select{}
box.space._schema:insert{"key1", 1}
xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id
row.HEADER.replica_id -- must be 1

test_run:cmd("switch master1")
box.space._cluster:select{}
box.space._schema:get{"key1"}

xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id
row.HEADER.replica_id -- must be 4


test_run:cmd("switch replica")
box.space._cluster:select{}
box.space._schema:get{"key1"}

xlog = require('xlog')
fio = require('fio')

list_dir = fio.listdir(box.cfg.wal_dir)
table.sort(list_dir)
path = fio.pathjoin(box.cfg.wal_dir, list_dir[#list_dir])
row = nil
for k, v in xlog.pairs(path) do if v.BODY.space_id == box.schema.SCHEMA_ID then row = v break end end
row.BODY.space_id
row.HEADER.replica_id -- must be 4


-- cleanup
test_run:cmd("switch default")
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")

test_run:drop_cluster(SERVERS)