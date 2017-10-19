env = require('test_run')
test_run = env.new()
box.schema.user.grant('guest', 'replication')

--
-- gh-2798: transactional replication. Ensure box transactions are
-- atomic on replica.
--

s = box.schema.space.create('test', {engine = 'memtx'})
pk = s:create_index('pk')
s2 = box.schema.space.create('test2', {engine = 'vinyl'})
pk2 = s2:create_index('pk')
fiber = require('fiber')

-- Test multiengine transaction. It is batched into a single xlog
-- transaction but on a replica must not be applied as a single
-- box transaction.
test_run:cmd("setopt delimiter ';'")
function multiengine()
	fiber.create(function() s:replace{1, 2, 3} end)
	s2:replace{4, 5, 6}
end;
function multiddl()
	fiber.create(function()
		box.schema.space.create('test3')
		box.space.test3:create_index('pk')
	end)
	box.schema.space.create('test4')
end;
function mixed()
	fiber.create(function() box.space.test3:replace{1, 2, 3} end)
	box.schema.space.create('test5')
end;
function duplicate_key()
	box.begin()
	box.space.test3:insert{3,4,5}
	box.space.test3:insert{4,5,6}
	box.space.test3:insert{2,3,4}
	box.commit()
end;
test_run:cmd("setopt delimiter ''");

test_run:cmd("create server replica with rpl_master=default, script='replication/replica.lua'")
test_run:cmd("start server replica")
test_run:cmd("switch replica")

fiber = require('fiber')
while box.space.test == nil or box.space.test2 == nil do fiber.sleep(0.1) end
test_run:cmd("switch default")

multiengine()
multiddl()
mixed()
test_run:cmd("switch replica")

log = require('log')
-- Wait for multiengine batch.
max_sleep = 5
test_run:cmd("setopt delimiter ';'")
while box.space.test:count() ~= 1 and box.space.test2:count() ~= 1 and max_sleep ~= 0 do
	fiber.sleep(0.1)
	max_sleep = max_sleep - 0.1
end;
test_run:cmd("setopt delimiter ''");
if max_sleep == 0 then log.error('Error with multiengine') assert(false) end

-- Wait for multiddl batch.
max_sleep = 5
test_run:cmd("setopt delimiter ';'")
while box.space.test3 == nil or box.space.test4 == nil and max_sleep ~= 0 do
	fiber.sleep(0.1)
	max_sleep = max_sleep - 0.1
end;
test_run:cmd("setopt delimiter ''");
if max_sleep == 0 then log.error('Error with multiddl') assert(false) end

-- Wait for mixed: ddl + not ddl.
max_sleep = 5
test_run:cmd("setopt delimiter ';'")
while box.space.test5 == nil or box.space.test3:count() ~= 1 and max_sleep ~= 0 do
	fiber.sleep(0.1)
	max_sleep = max_sleep - 0.1
end;
test_run:cmd("setopt delimiter ''");
if max_sleep == 0 then log.error('Error with ddl + not ddl') assert(false) end

--
-- Test rollback on duplicate key. Key must not be duplicate on
-- master, but be duplicate on replica. In such a case a whole
-- transaction can not be applied.
--
box.space.test3:insert{2,3,4} -- This key will be sent by master.

test_run:cmd("switch default")
duplicate_key()

test_run:cmd("switch replica")
max_sleep = 5
test_run:cmd("setopt delimiter ';'")
while box.info.replication[1].upstream.status == 'follow' and max_sleep ~= 0 do
	fiber.sleep(0.1)
	max_sleep = max_sleep - 0.1
end;
test_run:cmd("setopt delimiter ''");
if max_sleep == 0 then log.error('Error with consistency') assert(false) end

box.info.replication[1].upstream.status
-- Rows from the master are not applied.
box.space.test3:select{}

test_run:cmd("switch default")
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")

box.schema.user.revoke('guest', 'replication')
s:drop()
s2:drop()
