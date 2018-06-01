# Replicas remapping

* **Status**: In progress
* **Start date**: 11-05-2018
* **Authors**: Ilya Markov @IlyaMarkovMipt <imarkov@tarantool.org>, \<other authors\>
* **Issues**: #3098

## Summary

Replica ids remapping is a procedure of assigning local replicas id
 against replicas id from master in row based replication.
 
## Background and motivation
The problem is inspired by admin issues, e.g. deleting, adding, reseting nodes in cluster
and cleaning up vector clocks, replicasets meta information.

When node is deleted from replicaset and replaced with new one, the new can receive the old replica_id.
After that the deleted replica is returned back. The replica ids on the returned node
will differ from the replica ids on the replicas in replicaset. So the vclocks will differ too.


## Detailed design

Remapping is implemented with two mapping structures. One is stored in _cluster and reflects
mapping uuid to local replicas id. 
Another one is stored in in-memory mapping remote replica ids to local replicas ids.

The second one belongs only to applier and used for substitution of vclocks and xrow_headers.

The mapping in applier is updated within either initial subscribe _cluster download.
 or replication of space _cluster.

As it can be noticed, spaces _cluster on different nodes may differ. 
So during the replication it's necessary to handle tuples from the _cluster table
in order to update mappings or add new tuples in _cluster with appropriate local_id.
So that the tuple must be changed if replica id is already in use on this replica.

That's why we introduce before_triggers on _cluster space. Each trigger is set per applier.
Another one trigger is global one. Is is set to maintain counter of local replica id. So far we
 decided that new replica id monotonically grows, because problems with replica id collission are not explored.
 Later, this assigning may be changed to something smarter. 

Global counter of replicas must be monotonic. With this requirement we can be sure, that
ids of deleted replicas won't be reused and vclocks won't be confused with it.

### Protocol of cluster update
We add another phase in replication between join and subscribe phases - 
initial _cluster download.

This phase is required because of the cases, when replica was separated from replicaset,
was deleted from _cluster on masters in replicaset or discovered new replicas so that _clusters 
differ on the separated replicas. 

 With receiving xlogs triggers also apply received xlogs (updating _cluster and applier's mapping).
After that all vclock sent to or received from master and xrow_headers are updated with regards to remapping.

So the _cluster space is downloaded twice.
We could avoid initial downloading with keeping crc of space somewhere and sending it within
 first connecting on subscribe.
## Problems and ways to overcome them

1. Problem with primary key in _cluster. So far primary key in _cluster is replica_id.
But as we want to update inside before triggers according to our local replica id assigning,
 we need to update this field. Nevertheless, it's prohibited to update primary key field inside before_triggers.

*Solution*:
 That's why we alter primary index to indexing uuid field. The second index we alter to indexing replica_id.


2. Problem with simultaneous appliers. When several appliers exist in one moment, several triggers
are set and each of them will be called. The problem is when the new tuple is delivered,
we want to handle it only once, therewith by the trigger set by the applier
 for which tuple has come for.
Therefore, we need to map tuples to appliers inside triggers.

*Solution*:
The idea we decided to implement is to add third field in tuples representing the uuid of replica it was sent.
With that we can decide whether this tuple was sent to the applier for which trigger was called,
simply comparing third field of tuple with applier->uuid.

3. Before triggers are not called on join operation, so we don't update some of our _cluster meta data.
 It's not a problem for mappings, because the joining replica doesn't have _cluster at all.
 But it's problem for local replica id counter. It should be updated on each new replica added.
 
 *Solution*: On the call of _cluster trigger(the one is not assigned to any applier),
  we check if we have already updated local replica id counter. 
  If yes, we use its value.
  Otherwise, we use the maximum replica id from _cluster table.
  
  Also the problem here is that the third field is not updated on join. 
  But as such not-updated tuple are written in snapshots and in future can be handled only in join again,
  this field will be unnecessary.

4. When should we set up the triggers? The initial data reception at join phase does not require mapping
 because within that phase node doesn't have an empty _cluster. But on subscribe or on recovery triggers are required.

*Solution*: Trigger used for global counter is set on bootstrap, 
the others are set either in join after initial data receiving, or in subscribe phase.

5. How to handle global counter? Global counter is used to assign new replicas ids.
We have to assign it unique in order not to overlap it with other alive and disabled replicas.

*Solution* Let's assign replica counter `RC`. 
On new replica registration we calculate `RC = max(max_id(_cluster), RC) + 1`
With this formula we take into account the fact that triggers are not called on initial data reception during join phase,
and the fact that replicas may be deleted.

6. Another issue is the tuples whose third field(source uuid) is unknown for replica.

In this case we would spoil _cluster, because we don't have trigger to handle this tuple.

*Solution*: Skip such tuples. We need this tuples from _cluster mostly only for tracking vclocks.
But if replica doesn't have applier for the replica with such uuid then this replica should not be vclock representation.

## Alternatives

Possible alternative was to use the uniqueness of UUID and
 store uuid instead of replica id in vclocks and xrows. In this way, there would be no need in remapping, as we could easily distinguish the replica.
 But the approach consumes much more memory and message size than previous one.
 Size of uuids is bigger in magnitude than simple identifiers. 
