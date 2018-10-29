-- ctl.lua (internal file)

-- checks whether given replica is dead
local function is_dead(replica)
    -- self
    if replica.uuid == box.info.uuid then
        return false
    end
    -- no applier no relay
    if replica.upstream == nil and replica.downstream == nil then
        return true
    end
    -- disconnected or stopped relay
    if replica.upstream == nil and replica.downstream ~= nil then
        if replica.downstream.status == 'disconnected' or replica.downstream.status == 'stopped' then
            return true
        else
            return false
        end
    end
    -- in other cases replica is alive
    return false
end

-- return list of replicas suspected to be dead
function replicaset_list_inactive()
    local inactive_list = {}
    local replicaset = box.info.replication
    for i, replica in pairs(replicaset) do
        -- current replica is alive
        if is_dead(replica) then
            table.insert(inactive_list, replica.uuid)
        end
    end
    return inactive_list
end

-- throw away any replica from system space
function replica_prune(uuid)
    if uuid == nil then
        error("Usage: replica_displace([uuid])")
    end
    box.space._cluster.index.uuid:delete{uuid}
end

-- delete table of dead replica obtained from replicaset_list_inactive() or
-- formed by admin
function replicaset_purge(uuid_table)
    if uuid_table == nil then
        error("Usage: replicaset_trim([uuid_table])")
    end
   for i in pairs(uuid_table) do
        print("Deleting replica with uuid ".. i.. " "..uuid_table[i])
        replica_prune(uuid_table[i])
    end
end
