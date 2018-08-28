#!/usr/bin/env tarantool
require('console').listen(os.getenv('ADMIN'))

events = {}

local ctl_const = box.ctl_event.const()
local recovery_status = nil

local function before_replace(old, new)
    if recovery_status == ctl_const.RECOVERY_SNAPSHOT_START or
       recovery_status == ctl_const.RECOVERY_SNAPSHOT_DONE then
       -- local files
       return new
    end
    if new == nil then
        return new
    end
    local k = {new:unpack()}
    table.insert(k, 'R')
    return box.tuple.new(k)
end


local function ctl_event_trigger(event)
    -- register the event
    table.insert(events, event)
    if event.type == ctl_const.RECOVERY then
        recovery_status = event.status
    end
    if event.type == ctl_const.SPACE and
       event.action == ctl_const.SPACE_CREATE then
        if event.space_id > 511 then
            local space = box.space[event.space_id]
            space:before_replace(before_replace)
        end
    end
end

box.ctl_event.on_ctl_event(ctl_event_trigger)

box.cfg({
    listen              = os.getenv("LISTEN"),
    replication         = os.getenv("MASTER"),
    memtx_memory        = 107374182,
    replication_connect_timeout = 0.5,
})
