cluster
===

nodes ring:
- cluster configuration contains array of replicas, sorted by replica id,
- object is stored in rf (replication factor) replicas, starting by firsReplica for given hash,
- to calculate first replica (as well as followin), only cluster configuration, objectHash and algorithm is needed,


object -> firstReplica algorithm
---
- objectHash is calculated for object as SHA-256 (object bytes)
- slot is calculated form objectHash:
    - i1 .. i3 are first 3 32 int values from objectHash (seen as int[])
    - <code>slot = ((i1 ^ i2 ^ i3)&0x7fffffff) % replSlotSize</code>
- slot are mapped to first replica (r1):
    - <code> r1 = replicas_array[  (slot * nodes_count) / replSlotSize] </code>
    
replication
---

- when new object is inserted (non existing hash), 
replica sends this object immediatly to other involved replicas,
along with its seqId and previously send seqId for target replica
    - this can be turned of by put parameter,
    - after reception, next replica does not resend that
    - replica acknowledges by returning its seqId for that object
    - put calls ends (is confirmed) after defined number of positive acks
    
- heartbit
    - replicas send heartbits to other ones (it can be limited to that sharing keys)
    - heartbit includes current state (seqIds) for tartget replica
    
- when trying to insert obejct to "inproper" replica, replica can:
    - answer with error (this is implemented for start)
    - propagate call to proper replica
    
    
In this model nothing prevents clients from trying multiple times, until success or timeout.
 All put operations are idempotent (or at least should be), due to SHA-256 key.
 
change stream
---
upon request, replica sends stream (or list) of changes. 
- it is limited to a number of entries,
- it can be filtered to changes for specific replica,
- it contains last seqId (after filtering) 


- each replica stores:
    - map<key,value> of objects,
    - ordered seqence of ids it stores,
    - for each other replicas, that share the some keys:
        - last continuously applied/existing seqId (it can be sure it replicated all keys up to seqId from that replica)  
        - last seqId of objects that applies to that replica (so the other replica )

