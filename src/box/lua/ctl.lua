-- ctl.lua (internal file)

dead_gap = 0
rw_gap = 0

local function is_dead(replica)
    -- no information about applier and relay
    if replica.lar == nil and replica.law == nil then return true end

    -- time between last active read and now exceeds dead_gap
    if replica.lar > dead_gap then return true end

    -- time between last active write and now exceeds dead_gap
    if replica.law > dead_gap then return true end

    -- something happened to relay or applier
    if math.abs(replica.lar - replica.law) > rw_gap then return true end

    return false
end

-- return list of replicas suspected to be dead
function replicaset_list_wasted()
    dead_gap = box.cfg.replication_dead_gap
    rw_gap = box.cfg.replication_rw_gap
    if dead_gap == 0 or rw_gap == 0 then
         error("replication_dead_gap and replication_rw_gap must be set")
    end
    local wasted_list = {}
    local replicaset = box.info.replication
    for i, replica in pairs(replicaset) do
        -- current replica is alive
        if replica.uuid ~=  box.info.uuid and is_dead(replica) then
            table.insert(wasted_list, replica.uuid)
        end
    end
    return wasted_list
end

-- throw away any replica from system space
function replica_displace(uuid)
    if uuid == nil then
        error("Usage: replica_displace([uuid])")
    end
    box.space._cluster.index.uuid:delete{uuid}
end

-- delete table of dead replica obtained from replicaset_list_wasted() or
-- formed by admin
function replicaset_trim(uuid_table)
    if uuid_table == nil then
        error("Usage: replicaset_trim([uuid_table])")
    end
   for i in pairs(uuid_table) do
        print("Deleting replica with uuid ".. i.. " "..uuid_table[i])
        replica_displace(uuid_table[i])
    end
end
