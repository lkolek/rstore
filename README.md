rstore: replicated object store
====================================

currently in alpha stage

------

rstore provides simple to install, embedable, replicated store functionality.
This is object store with following capabilities:
- Append - only,
- HA (highly avaible),
- RF (support replication factor),
- auto healing (after nodes returns),
- eventually consistent

There is only one common shared configuration, that must be avaible for nodes (and clients) of the cluster.
Currently there are no plans for cluster rebalance functionality etc.

desing
------

object ID
- deterministic (by object data)
- for given object (byte[]), it is hash (SHA-2).
- must be calculated when inserting

replication
- "Replica slot": number (0 .. MAX_REPL_SLOT)
- one simple and common, deterministic pseudo-random function that returns first replica slot,
- based on previous, a N of replicas are returned (by mapping replica slot space to replicas defined in config) - continuous hashing

client / replication / guarantees
- client must have current cluster structure. It can be obtained from any cluster node (as for now, no important changes to cluster config are possible)
- based on that, client insert object to any of cluster nodes responsible for particular hash
    - if RF is required, client can wait for replication to be acknowledged or insert object to required number of responsible replicas
- in case of failure, client should retry operation. Inserting is idempotent.

todo
-----
- [x] basic cluster conf
- [x] hash (objectId)
- [x] objectid to slot
- [ ] slot to replica
- [ ] node storage
- [ ] node object changes protocol / stream

license
----

TBD: license files

Dual licensing model: Apache 2 License or LGPL v. 3

Please be noted that some of dependecies uses LGPL license model.

