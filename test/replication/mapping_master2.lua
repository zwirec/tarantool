#!/usr/bin/env tarantool

-- get instance name from filename (mapping_master2.lua => mapping_master2)
local INSTANCE_ID = string.match(arg[0], "%d")

local SOCKET_DIR = require('fio').cwd()
local function instance_uri(instance_id)
    return SOCKET_DIR..'/mapping'..instance_id..'.sock';
end

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = instance_uri(INSTANCE_ID);
    replication = {
        instance_uri(1);
        instance_uri(2);
    };
    instance_uuid = arg[1];
})

test_run = require('test_run').new()
engine = test_run:get_cfg('engine')

box.once("bootstrap", function()
    box.schema.user.grant("guest", 'replication')
end)
