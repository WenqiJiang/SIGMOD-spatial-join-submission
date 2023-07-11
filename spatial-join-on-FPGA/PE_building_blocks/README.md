# PE building blocks

This folder contains the building blocks for spatial join.


## node_join_PE_complex

Joining two pages A and B. It considers the synchronous traversal procedure:

It first loads the meta data of the pages, and decide the entry numbers as well as whether the pages are leaf.

If both pages are leaf nodes or both are directory nodes, then run join between them (up to N^2 results).

If only one of the pages is a leaf node, then run join between all the directory children with the leaf node MBR (up to N results). 


## node_join_PE_simple

Joining two pages A and B. The page has simple object structure including the obj ID and the four boundaries.

## node_join_PE_simple (failed HLS compilation)

Try to unroll the join by using pragma.

```
            for (int n = 0; n < page_entry_num; n++) {
#pragma HLS pipeline II=1
#pragma HLS unroll factor=16
```

The HLS compilation stuck there for one day, so it's probably non-trivial to just parallelize the join loop... We need to instantiate multiple PEs instead.
