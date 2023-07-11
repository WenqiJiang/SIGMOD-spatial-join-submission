# Spatial Join on FPGAs

## Node join PEs

### node_join_PE_simple

A simple join PE. It takes two nodes (pages) as inputs, both are raw data nodes, with no meta info
Assuming both nodes are data nodes, both no meta info. 

Note: the write function can be the bottleneck (~7 cycles per write), as the while loop with termination detection cannot be inferred as burst write. 
### (failed) node_join_PE_simple_unroll

Add an unroll factor of 16 in the comparison loop, and using 16 output FIFO. Results: HLS does not proceed at all... 

### node_join_PE_complex

A complex join PE. It takes two nodes (pages) as inputs, both contains meta info (whether they are leaf nodes, number of entries within the node, node's mbr). Then the results depends on the type: if only one of them is leaf, join the leaf's MBR with the other data node's children. Otherwise, if both nodes are the same type, just join their children/data. 

Note: the write function can be the bottleneck (~7 cycles per write), as the while loop with termination detection cannot be inferred as burst write. 
