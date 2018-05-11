# Replicas remapping

* **Status**: In progress
* **Start date**: 11-05-2018
* **Authors**: Ilya Markov @IlyaMarkovMipt <imarkov@tarantool.org>, \<other authors\>
* **Issues**: #3098

## Summary

Replicas remapping is a procedure of assigning new replicas id on the node
within a cluster with several replicasets in it.

The solution is based on simple idea of mapping server uuid not only to replicaset local id,
but to global in context of united replicasets identifier.
 
## Background and motivation
Currently one node can not be a part of several replicasets withing its life.
This fact is caused mostly by confusing of replicas' identifiers which may overlap
and cause wrong usage of vclocks and xlog.

Motivation for uniting different replicasets in one replica lies in possible usage 
this replica as an aggregator of data from thess replicasets.
The possible usage of this aggregator is following

```lua
-- The code on aggregator.
-- Download all data(snapshot, xlogs) from this replicaset and subscribe on it.
box.cfg{replication = {uri1, uri2}}

-- Download data from another replicaset and subscribe on it.
box.cfg{replication = {uuid1, uuid2, uuid3, uuid4}}
```

In this case aggregator node will have data from both replicasets(the first one 
is `uuid1`, `uuid2`, the second = `uuid3`, `uuid4`), 
whereas the replicasets have no clue about each other. 
Conflicts in data can be resolved with before_replace trigger.

The problem with system spaces which might appear during join phase is still not solved.
## Detailed design

Remapping is implemented with set of maps where each local id(local inside replicaset) is assigned to global id.
By term global, we mean here an identifier which is unique for the replicasets,
to which this node subscribed whenever.

This mapping is stored persistent in system space _cluster.

Proposed new format of _cluster is following:

```lua
format = {}

-- Id of replica in united replicaset.
format[1] = {'global_id', 'unsigned'}
-- UUID of replica
format[2] = {'uuid', 'string'}
-- Id of replica within its replicaset
format[3] = {'local_id', 'unsigned'}
-- Replicaset id. Used in deserialization.
format[4] = {'replicaset_id', 'unsigned'}
```

### Protocol of cluster update
*The protocol is currently implemented as an extension to subscribe phase.*

Replica:

Before subscribe initial message.

1. Get local mapping for connected master. If it doesn't exist create one.
2. Calculate crc32 check sum for local_id:uuid pairs.
3. Remap local vclock before initial message.
4. Initial subscribe message now looks like following:
    * replicaset_uuid
    * local replica uuid
    * vclock of this replica in the required replicaset
    * crc32 of local information of replicaset.

Master:
5. Receive initial message. Calculate crc32 of cluster information. Compare it with checksum in message.
6. If checksums are not identical, send cluster info in following way:
    * Initial cluster info message contains local_id, uuid and length of replicaset
    * All other messages contain local_id, uuid of replicas.
    * So number of sent messages equals size of replicaset. 
If checksums match, initial message contains length equal to 0 and no other cluster info messages are sent

Replica:
7. Receive the response about the relevancy of local information of replicaset.
    If the initial response contains replicaset length equal to 0, there is no need in cluster update.
    Otherwise, update local mapping with incrementing global counter if new uuid was found,
     receiving other update messages.

Master:
9. Continue subscribe protocol.

Replica:
10. Continue subscribe protocol with one correction:
Replace replica_id field in replicated log with its mapped value in local mapping.

## Rationale and alternatives

Possible alternative was to apply the uniqueness of UUID and
 store uuid instead of replica id in vclocks and xrows. In this way, there would be no need in remapping, as we could easily distinguish the replica.
 But the approach consumes much more memory and message size than previous one.
 Size of uuids is bigger in magnitude than simple identifiers. 
