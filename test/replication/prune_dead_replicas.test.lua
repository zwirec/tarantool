test_run = require('test_run').new()
replica_set = require('fast_replica')
fiber = require('fiber')

box.schema.user.grant('guest', 'replication')
test_run:cmd("push filter 'lsn: [0-9]+' to 'lsn: <lsn>'")
test_run:cmd("push filter 'uuid: .*' to 'uuid: <uuid>'")
test_run:cmd("push filter 'idle: .*' to 'idle: <idle>'")
test_run:cmd("push filter 'peer: .*' to 'peer: <peer>'")
test_run:cmd("push filter 'lag: .*' to 'lag: <lag>'")
test_run:cmd("push filter 'vclock: .*' to 'vclock: <vclock>'")
test_run:cmd("push filter '(error: .builtin/.*[.]lua):[0-9]+' to '\\1'")

box.replication.get_alive_replicas("name")

alive = box.replication.get_alive_replicas(0.001)
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 1


replica_set.join(test_run, 2)
while box.space._cluster:len() ~= 3 do fiber.sleep(0.001) end
box.space._cluster:len() == 3
box.info.replication

test_run:cmd("switch replica1")
box.space._cluster:len() == 3
box.info.replication

test_run:cmd("switch default")
alive = box.replication.get_alive_replicas(0.001)
box.replication.prune_dead_replicas(alive)

-- nothing is deleted
box.space._cluster:len() == 3
box.info.replication

test_run:cmd("switch replica2")
box.cfg{replication = {}}
test_run:cmd("switch default")

while box.info.replication[3].downstream ~= nil and box.info.replication[3].downstream.status ~= "stopped" do fiber.sleep(0.001) end
alive = box.replication.get_alive_replicas(0.001)
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 2
box.info.replication

test_run:cmd("restart server replica2 with cleanup=1")
while box.space._cluster:len() ~= 3 do fiber.sleep(0.001) end
test_run:cmd("switch default")

ch = fiber.channel(1)
-- The function get_alive_replicas consists of two checks of box.info.replication.
-- Replica is considered to be alive if it is alive in one of these checks.
-- Test these checks. In these test we highly rely on the fact that
-- replica will be considered disconnected or restarts faster than
-- specified timeout.

-- First check sees replica alive, second sees it dead. Test this replica must not be deleted.
func = function(timeout) ch:put(1) ch:put(box.replication.get_alive_replicas(timeout)) end
f = fiber.create(func, 0.5)
ch:get()

test_run:cmd("switch replica2")
box.cfg{replication = {}}
test_run:cmd("switch default")
while box.info.replication[3].downstream ~= nil and box.info.replication[3].downstream.status ~= "stopped" do fiber.sleep(0.001) end
alive = ch:get()
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 3

-- First check sees replica dead, second sees it alive. Test this replica must not be deleted.
f = fiber.create(func, 1.0)
ch:get()
test_run:cmd("restart server replica2 with cleanup=1")
test_run:cmd("switch default")
alive = ch:get()
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 3

test_run:cmd("switch replica1")
test_run:cmd("stop server default")
while box.info.replication[1].upstream.status == "follow" do fiber.sleep(0.001) end

alive = box.replication.get_alive_replicas(0.001)
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 1
box.info.replication

test_run:cmd("deploy server default")
test_run:cmd("start server default")
test_run:cmd("switch default")

test_run = require('test_run').new()
replica_set = require('fast_replica')
replica_set.drop_all(test_run)
-- cleanup. Don't revoke guest privilege as it was lost on deploy.

alive = box.replication.get_alive_replicas(0.001)
box.replication.prune_dead_replicas(alive)
box.space._cluster:len() == 1
box.info.replication
