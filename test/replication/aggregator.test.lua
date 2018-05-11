test_run = require('test_run').new()

test_run:cmd("setopt delimiter ';'")
test_run:cmd("setopt delimiter ''");

test_run:cmd("create server master1 with script='replication/master1.lua'")
test_run:cmd("create server replica with rpl_master=default, script='replication/replica.lua'")

test_run:cmd("switch default")
box.schema.user.grant('guest', 'replication')
_ = box.schema.space.create("test", {engine=test_run:get_cfg('engine')})
_ = box.space.test:create_index("primary")
box.space.test:insert{1}

test_run:cmd("start server master1")
test_run:cmd("switch master1")
box.schema.user.grant('guest', 'replication')
_ = box.schema.space.create("test", {engine=test_run:get_cfg('engine')})
_ = box.space.test:create_index("primary")

test_run:cmd("start server replica")
test_run:cmd("switch replica")
USER="guest"
PASSWORD=""
function get_uri(port) return USER..":"..PASSWORD .. "@localhost:" .. port end
box.space.test:select{}
test_run:cmd("set variable default_port to 'replica.master'")
test_run:cmd("set variable master1_port to 'master1.listen'")
box.cfg{replication={get_uri(default_port), get_uri(master1_port)}}

test_run:cmd("switch master1")
box.space.test:insert{2}

test_run:cmd("switch default")
box.space.test:insert{3}

test_run:cmd("switch replica")
box.space.test:select{}

test_run:cmd("switch default")
box.space.test:drop()
box.schema.user.revoke("guest", "execute", "role", "replication")
test_run:cmd("stop server replica")
test_run:cmd("stop server master1")
test_run:cmd("cleanup server replica")
test_run:cmd("cleanup server master1")

-- need cleanup default before other tests
test_run:cmd('restart server default with cleanup=1')
