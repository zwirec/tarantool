# Proxy

* **Status**: In progress
* **Start date**: 19.03.2018
* **Authors**: Vladislav Shpilevoy @Gerold103 \<v.shpilevoy@tarantool.org\>, Konstantin Osipov @kostja \<kostja@tarantool.org\>
* **Issues**: #2625, #3055

## Summary

Proxy is a module to route requests from slaves to a master. It is built-in, configured via `netbox`, and can work both on a master and on a slave. Proxy on a slave resends all requests to a master. On a master a proxy does nothing.

## Background and motivation

A client must not know which instance is master, and which is replica - these details must be hidden behind a proxy. A client must be able to send a request to any cluster member, and to get a correct result, even for write requests. This is a preparation for a standalone proxy.

A killer feature of a proxy joined with a storage is that on a master it has no overhead. It works in the same process as a storage, and can send requests directly to transaction processing thread. On a slave a proxy works in a network thread, in which it holds connections to a master. Exactly connection**s** in a common case, not connection, because for each user using a proxy an own connection is needed to separate user rights on various objects and actions. For example, if a cluster has 10 users, and 3 send requests to a slave, then the slave's proxy holds 3 connections to a master.

Moreover, proxy merged with a storage has access to space `_user` with password hashes, which can be used to transparently authenticate users on a master. More detailed description in the next section.

Besides authentication, a proxy must translate syncs for each request in a multiplexed connection. When multiple client-to-proxy connections share a single fat proxy-to-master connection, syncs in requests from different clients can be duplicated. So the proxy must change sync to its own unique value when forwarding a request to a master, and change sync back in a response.

API:
```Lua
-- At first, turn on proxy.
netbox = require('net.box')
proxy = netbox.listen(...)
-- Now the proxy accepts requests, but does not forward
-- them.
box.cfg({
	-- When a proxy is up, the box.cfg
	-- can do not care about listen.
	listen = nil,
	replication = { cluster_members },
	-- Other options ...
})
```

Proxy does automatic failover, when `box.ctl.promote()` is called.

## Detailed design

### Architecture

```
client, user1 ------*
 ...                 \           proxy              master
client, user1 --------*----------* - *----------------*
                         ---- SYNC -> proXYNC ---->
                         <--- SYNC <- proXYNC -----
```
A proxy lives entirely in IProto (network) thread. On start it creates guest connections to all cluster members. Despite of the fact that a proxy sends all requests to a master, it must be able to do fast failover to one of replicas. So it must hold connections to slaves too. Salts, received from slaves and from a master, are stored by proxy to use them for authentication schema below.

#### Authentication

To understand how a proxy authenticates a user, the one must recall a Tarantool authentication protocol, described below:
```
SERVER:  salt = create_random_string()
         send(salt)

CLIENT:  recv(salt)
         hash1 = sha1(password)
         hash2 = sha1(hash1)
         reply = xor(hash1, sha1(salt, hash2))

         send(reply)

SERVER:  recv(reply)
         hash1 = xor(reply, sha1(salt, hash2))
         candidate_hash2 = sha1(hash1)
         check(candidate_hash2 == hash2)
 ```
 
A proxy has access to hash2, which is stored in space `_user`. Proxy replies to a just connected user with a local salt (!= master or other slaves salt). A client responds with `reply = xor(hash1, sha1(salt, hash2))`. The proxy knows both salt and hash2 and can calculate `hash1 = xor(reply, sha1(salt, hash2))`. Now a proxy can emulate client AUTH request to a master: `auth = xor(hash1, sha1(master_salt, hash2))`.

When a new client connects to a proxy, it searches for an existing connection to a master for a user, specified in client auth request. If found, then the new client's requests are forwarded to this connection. If no existing connection is found, then the proxy establishes a new one using master salt, calculated hash1 and hash2 to authenticate the new user.

#### Sync translation

If a proxy-to-master connection serves one client-to-proxy connection, then `sync` translation is not needed - there are no conflicts. When a proxy-to-master connection serves multiple client-to-proxy connections, the proxy stores and maintains increasing `sync` counter. Consider the communication steps:
1. A client sends a request to a proxy with `sync = s1`;
2. A proxy remembers this `sync`, changes it to `sync = s2`, sends the request to a storage;
3. A response with `sync = s2` is received from the storage. The proxy replaces `s2` back to `s1` and sends the response to the client.

A proxy-to-master connection stores a hash of original and translated syncs, and removes a record from the hash, when a master respond `IPROTO_OK` or `IPROTO_ERROR`.  A special case is `IPROTO_PUSH` - a push does not finish a request, so on a push a syncs hash is not changed.

#### Queuing

Consider one proxy-to-master connection. To prevent mixing parts of multiple requests from different client-to-proxy connections, a proxy must forward requests one by one. To do it fairly, a proxy-to-master connection has a queue. In the queue client-to-proxy connections are stored, those sockets are available for reading.

When a client socket with no available data becomes available for reading, it stands at the end of the queue. First client in the queue after sending ONE request is removed from a queue. If it has more requests, then it stands at the end of the queue to send them. It guarantees a fairness if one client will be always available for reading.

To speed up `sync` translation, it can be done right after receiving a request from a client, with no waiting until a proxy-to-master connection is available for writing. It allows to do not dawdle with `sync`s when a client appears in the front of the queue.

## Rationale and alternatives

Consider another ways to implement some proxy parts. Lets begin from authentication. Authentication on the most of proxies of another DBMSs is not transparent - they store user names and passwords in a local storage. At first, it is not safe, and at second, passwords on a proxy can malsync with actuall passwords, that requires to reconfigure the proxy. The Tarantool proxy authentication is more useful, since it does not require any credentials configuring.

Another point, is that the first Tarantool proxy version can not be separated from a storage. This is slightly ugly, but it allows transparent authentication, and overhead-free proxy on a master. A standalone proxy is a next step.

The next point at issue is how IProto thread would read auth info from TX thread, when a new client is connected. There are several alternatives:
* Protect write access to user hash with a mutex. TX thread locks the mutex when writing, and does not lock it for reading, since TX is an exclusive writer. IProto thread meanwhile locks the mutex whenever it needs to read `hash2` to establish an outgoing connection;<br>
**Advantages:** very simple to implement. It is not a performance critical place, so mutex looks ok;<br>
**Shortcomings:** it is not a common practice in Tarantool to use mutexes. It looks like a crutch;
* Maintain a copy of the user hash in iproto thread, and propagate changes to the copy via cbus, in new on_replace triggers added to the system spaces;<br>
**Advantages:** no locks;<br>
**Shortcomings:** it is possible, then a client connects, when a hash copy in IProto thread is expired. And it is relatively hard to implement;
* Use a single copy of the users hash in TX thread, but protect access to this copy by `fiber.cond` object local to IProto thread. Lock/unlock the cond for IProto thread by sending a special message to it via cbus;<br>
**Advantages:** no mutexes;<br>
**Shortcomings:** the hash can expire in the same scenario, as in the previous variant. Hard to implement;
* We use persistent schema versioning and update the state of IProto thread on demand, whenever it notices that schema version has changed;<br>
**Advantages:** no locks, easy;<br>
**Shortcomings:** at first, it does not solve the problem - how to get the changes from TX thread? At second, schema is not updated, when a users hash is changed. And it is strange to update version for it.
* We kill iproto thread altogether and forget about the problem of IProto-TX synch forever (it is not a big patch and it may well improve our performance results).<br>
**Advantages:** in theory, it could speed up the requests processing;<br>
**Shortcomings:** it slows down all slaves, since proxy on a slave works entirely in IProto thead. If a proxy is not in a thread, then on a slave it will occupy CPU just to encode/decode requests, send/receive data, do `SYNC`s translation. When a routing rules will be introduced, it will occupy even more CPU;
* On each new client connection get needed data from a user hash using special cbus message;<br>
**Advantages:** simple, lock-free, new clients can not see expired data, proxy auth is stateless<br>
**Shortcomings:** ?
