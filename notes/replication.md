replication
==

replication Ra -> Rb

Simple model:

- int replicatedToRbSeqId
- queryIds(replicatedToRbSeqId, CNT)
    - for each
        - if not exists & applicable
            - get, 
                - insert + update own seqId / seqIds
    - after all responses (incl get)
        - update replicatedToRbSeqId to last continuous positive
    - repeat (if needed, delay)
    


  