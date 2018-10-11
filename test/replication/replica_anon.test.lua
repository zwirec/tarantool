test_run = require('test_run').new()

box.schema.user.grant("guest", "replication")
_ = box.schema.space.create("test")
_ = box.space.test:create_index("pk")

for i = 1, 100 do box.space.test:auto_increment{} end

test_run:cmd("create server replica_anon with rpl_master=default, script='replication/replica_anon.lua'")
test_run:cmd('start server replica_anon')
-- Check anonymous join + subscribe.
test_run:cmd("switch replica_anon")
box.space.test:count()
box.info.replication[1].upstream.status
test_run:cmd("switch default")

test_run:cmd("stop server replica_anon")

for i = 1, 100 do box.space.test:auto_increment{} end
-- Check anonymous subscribe without join.
test_run:cmd("start server replica_anon")
test_run:cmd("switch replica_anon")
box.space.test:count()
test_run:cmd("switch default")

for i = 1, 100 do box.space.test:auto_increment{} end

-- Check following updates.
test_run:cmd("switch replica_anon")
while box.space.test:count() < 300 do fiber.sleep(0.01) end
box.space.test:count()
test_run:cmd("switch default")

-- Check display of anonymous replicas in box.info.
box.info.replication[#box.info.replication].anonymous

-- Cleanup.
test_run:cmd("stop server replica_anon")
test_run:cmd("cleanup server replica_anon")
box.schema.user.revoke("guest", "replication")
box.space.test:drop()
