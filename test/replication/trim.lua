#!/usr/bin/env tarantool

-- get instance name from filename (trim1.lua => trim1)
local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()
local TIMEOUT = tonumber(arg[1])
local CON_TIMEOUT = arg[2] and tonumber(arg[2]) or 30.0

local function instance_uri(instance_id)
    --return 'localhost:'..(3310 + instance_id)
    return SOCKET_DIR..'/trim'..instance_id..'.sock';
end

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = instance_uri(INSTANCE_ID);
    replication_timeout = TIMEOUT;
    replication_connect_timeout = CON_TIMEOUT;
    replication = {
        instance_uri(1);
        instance_uri(2);
        instance_uri(3);
        instance_uri(4);
    };
})

box.once("bootstrap", function()
    local test_run = require('test_run').new()
    box.schema.user.grant("guest", 'replication')
    box.schema.space.create('test', {engine = test_run:get_cfg('engine')})
    box.space.test:create_index('primary')
end)


fiber = require('fiber')
function wait()
   local i = 80
   while i~= 0 do fiber.sleep(0.05) i = i - 1 end
end

function find_wasted_by_law(uuid)
    for i, info in pairs(box.info.replication) do
        if info.uuid == uuid then
            return info.law > box.cfg.replication_dead_gap
        end
    end
    return false
end

function find_wasted_by_lar(uuid)
    for i, info in pairs(box.info.replication) do
        if info.uuid == uuid then
            return info.lar > box.cfg.replication_dead_gap
        end
    end
    return false
end

function find_wasted_by_rw(uuid)
    for i, info in pairs(box.info.replication) do
        if info.uuid == uuid then
            return math.abs(info.law - info.lar) > box.cfg.replication_rw_gap
        end
    end
end
