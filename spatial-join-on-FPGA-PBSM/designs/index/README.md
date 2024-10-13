# Indexed Nested Loop Join FPGA Implementation

This folder contains the FPGA implementation of the Indexed Nested Loop Join (INLJ) algorithm (in the `simple` folder). 

## 1. Introduction

The Indexed Nested Loop Join (INLJ) is an enhancement of the standard Nested Loop Join (NLJ), designed to perform spatial join operations more efficiently by using an R-tree index for dataset B. The index allows the join to focus only on potentially relevant comparisons, reducing unnecessary operations and speeding up the overall process.

## 2. Main Features

- **R-tree Index Integration**: Uses an R-tree index for dataset B to minimise the number of comparisons needed. The R-tree format follows the one introduced in SwiftSpatial.
- **Parallelised Dataflow**: Outer dataset data is streamed into the FPGA using a simple sequential read unit.
- **Dynamic Scheduler**: Balances workload across multiple processing elements (PEs) by distributing tasks to the available processing elements (PEs).
- **R-tree Traversal**: The join unit processes a page of objects from dataset A against the R-tree index for dataset B. 
- **Tree Node Memory Controller**: The memory controller fetches the R-tree nodes requested by the join unit and sends them back to the requesting PEs for processing.
- **Collect/Write Unit**: Gathers results from the PEs and writes them back to memory in a burst, similar to the NLJ.

## 3. Compiling for FPGA (Hardware)

To compile the implementation for different configurations (with varying numbers of join units), follow these steps:

1. **Run the `copy_and_modify.sh` Script**:
   This script creates directories for each configuration with a different number of join units (N = 1, 2, 4, 8, 16). You may need to run `chmod +x copy_and_modify.sh` to give execute permissions to the script.

   ```bash
   ./copy_and_modify.sh
   ```

2. **Run the `create_screens.sh` Script**:
   This script will create screen sessions and initiate compilation for the hardware (FPGA) in each configuration folder. The script runs `make cleanall` and `make all TARGET=hw` for N = 1, 2, 4, 8 and 16 join units.

   ```bash
   ./create_screens.sh
   ```

   This will start multiple screen sessions, each compiling for a different configuration.

## 4. Running the Implementation

### Arguments

The INLJ implementation expects several command-line arguments to run:

```bash
Usage: ./host xclbin/executor.hw.xclbin <<<data_file_A>>> <<<tree_file_B>>> <<<tree_max_level>>> <<<tree_max_node_entries_count>>> <<<max_num_results>>>
```

- `<<<data_file_A>>>`: Path to the first dataset (Dataset A).
- `<<<tree_file_B>>>`: Path to the serialised R-tree file for dataset B.
- `<<<tree_max_level>>>`: Maximum depth (level) of the R-tree.
- `<<<tree_max_node_entries_count>>>`: Maximum number of entries per R-tree node.
- `<<<max_num_results>>>`: (Optional) Maximum number of results allowed to be collect. Defaults to 1024.

### Example

```bash
./host xclbin/executor.hw.xclbin ./data/data_A.bin ./data/tree_B.bin 2 16 2048
```

This command runs the Indexed Nested Loop Join algorithm on datasets A and B using the R-tree index, with a maximum of 2048 results allowed.

## 5. Output

### What a Run Prints

The run will print key metrics for performance evaluation, similar to the NLJ implementation:

- **Kernel Time**: The time taken by the FPGA kernel to execute.
- **Total Time**: The total time from the start to the end of execution, including data transfer, kernel execution, and result collection.

### Example Output

```plaintext
Reading arguments...
Tree max level: 5
Tree max node entries count: 16
Max number of results: 2048
...
Starting kernel...
Kernel time: 0.98 seconds
Total time: 1.20 seconds
```

The output provides an overview of the time spent in kernel execution (on the FPGA) and the total runtime of the operation.

### 6. Cache-enhanced Tree Node Memory Controller

To address the latency inefficiencies associated with persistent random memory access patterns, a **cache-enhanced index read unit** can be introduced to store recently accessed nodes in an on-chip cache. A sketch of a possible implementation is in ``cache/1/src/memory.hpp``.

- **Cache Strategy**: The cache controller first checks whether the requested node is present in the cache. If the node is found, it is immediately returned to the join unit, reducing access latency.
- **Cache Misses**: If the node is not found in the cache, the controller fetches the node from external memory, stores it in the cache (following an LRU policy) and streams it to the join unit.
- **Benefit**: This approach should be particularly beneficial for nodes at the upper levels of the R-tree, which are accessed more frequently.



