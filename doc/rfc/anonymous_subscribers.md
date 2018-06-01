# RFC Template

* **Status**: In progress
* **Start date**: 27-05-2018
* **Authors**: Markov Ilya @IlyaMarkovMipt \<imarkov@tarantool.org\>
* **Issues**: [3340](https://github.com/tarantool/tarantool/issues/3340)

## Summary

Allow replicas which are not upstream of some instance in replicaset to be anonymous.
By the term "anonymous replica" we imply here that the replica is not stored in _cluster table 
on nodes it is connected to.

## Background and motivation

This requirement is inspired by the wish to scale replicasets. But in some cases most of replicas
just download data and do not participate in a lifespan of replicaset. So they just waste space of _cluster
 spaces and network in message exchange. Also, it would be inconvenient for admins to delete
 died replicas from master's _cluster. 

## Detailed design

The idea is simple: register replicas in _cluster only on box.cfg{replication}.
On subscribe add replicas to in memory structure holding replicas, but not to space.

For replicas we have to explicitly add theirselves to _cluster on bootstrap 
after initial join reception.

The problem with read only replicas is left. We need to execute the latter point,
 but it is prohibited on read only replicas.