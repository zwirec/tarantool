test_run = require('test_run').new()
test_run:create_cluster(CLUSTER, 'promote')
test_run:wait_fullmesh(CLUSTER)
--
-- Test the case when two different promotions are started at the
-- same time. Here the initiators are box2 and box3 while box1 is
-- an old master and box4 is a watcher.
--
_ = test_run:switch('box1')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box2')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box3')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box2')
err = nil
ok = nil
_ = fiber.create(function() ok, err = promote_check_error() end)

_ = test_run:switch('box3')
err = nil
ok = nil
f = fiber.create(function() ok, err = promote_check_error() end)
while f:status() ~= 'suspended' do fiber.sleep(0.01) end
errinj.set("ERRINJ_WAL_DELAY", false)

_ = test_run:switch('box2')
errinj.set("ERRINJ_WAL_DELAY", false)
while not err do fiber.sleep(0.01) end
ok, err

_ = test_run:switch('box1')
errinj.set("ERRINJ_WAL_DELAY", false)
while promote_info().phase ~= 'error' do fiber.sleep(0.01) end
info = promote_info(error_safe_info)
info.comment = info.comment:match('unexpected message')
info

_ = test_run:switch('box3')
while not err do fiber.sleep(0.01) end
ok, err

--
-- Test that after all a new promotion works.
--
box.ctl.promote()
promote_info()

_ = test_run:switch('default')
test_run:drop_cluster(CLUSTER)
