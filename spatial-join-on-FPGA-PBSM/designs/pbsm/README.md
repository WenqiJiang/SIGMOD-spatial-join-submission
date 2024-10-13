# PBSM Join FPGA Implementation

This folder contains the FPGA implementation of the Partition-Based Spatial Merge (PBSM) Join algorithm. 

## 1. Introduction

PBSM is designed to handle large-scale spatial join operations by dividing datasets into manageable partitions and processing them in parallel on an FPGA.

## 2. Main Features

- **Partitioning of Datasets**: Datasets are partitioned into grid cells based on their spatial extents.
- **Host-FPGA Architecture**: The host code prepares the data, while the FPGA performs the intensive parallel processing.
- **Initial Partitioning and Refinement**: The host partitions datasets and refines partitions to ensure they fit into the FPGA's processing units.
- **Parallelised Processing**: Each partition is processed independently by multiple FPGA processing elements (PEs), leveraging OpenMP for parallelism in the host and pipelined operations on the FPGA.
- **Efficient 512-bit Page Processing**: Spatial objects are packed into 512-bit pages for efficient transfer and processing by the FPGA.
- **Scheduler (Dynamic/Static)**: Distributes tasks to the PEs on the FPGA. Dynamic scheduler does it based on the PE availability while the static one using a round-robin scheduler.

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

The PBSM Join implementation expects several command-line arguments to run:

```bash
Usage: ./host xclbin/executor.hw.xclbin <<<data_file_A>>> <<<data_file_B>>> <<<max_num_results>>> <<<num_initial_partitions_1d>>> <<<max_comparisons_per_partition>>>
```

- `data_file_A`: Path to the first dataset (Dataset A).
- `data_file_B`: Path to the second dataset (Dataset B).
- `max_num_results`: (Optional) Maximum number of results allowed to be collected. Defaults to 1024.
- `num_initial_partitions_1d`: (Optional) Number of initial partitions to divide the spatial domain into, in one dimension. Defaults to 10.
- `max_comparisons_per_partition`: (Optional) The maximum number of comparisons allowed per partition (i.e. |A| * |B| <= `max_comparisons_per_partition`). Defaults to 1000.

### Example

```bash
./host xclbin/executor.hw.xclbin ./data/data_A.bin ./data/data_B.bin 2048 10 1000
```

This command runs the PBSM Join algorithm on datasets A and B with a maximum of 2048 results, dividing the spatial domain into 10 initial partitions in one dimension and allowing up to 1000 comparisons within a partition.

## 5. Output

### What a Run Prints

During execution, the PBSM Join implementation provides detailed timing metrics for both the CPU and FPGA operations. The output includes:

- **Kernel Time**: The time spent executing the FPGA kernel.
- **Total Time**: The total execution time, including both CPU and FPGA processing.
- **CPU Times**: Breakdown of the time spent on various CPU operations, such as initialising the grid, partitioning objects, refining partitions and preparing the partitions for FPGA processing.
- **Results**: The number of results produced by the join operation.

### Example Output

```plaintext
...
Processing: DONE!
TIME[kernel_time]: 123.45 ms.
TIME[total_time]: 234.56 ms.
TIME[cpu_init_time]: 12.34 ms.
TIME[cpu_partition_time]: 45.67 ms.
TIME[cpu_refine_time]: 8.90 ms.
TIME[cpu_prepare_time]: 67.89 ms.
TIME[cpu_time]: 134.80 ms.
RESULTS: 1000
COMPLETED!
```

The printed times are in milliseconds (`ms`), and the final output shows the number of results (```RESULTS```) found by the join operation. This output helps to evaluate the performance of both the CPU-side preprocessing and the FPGA-side execution.

## 6. Dual Host-FPGA Architecture

### Host Code Implementation

The host code is responsible for preparing and partitioning the datasets. It calculates the spatial extents of the datasets, divides the domain into grid cells and assigns spatial objects to these cells based on their minimum bounding rectangles (MBRs). This initial partitioning is parallelised using OpenMP. If a partition is too large, a refinement step further subdivides it to ensure it fits within the FPGA's buffer size.

Once the data is partitioned, it is packed into 512-bit pages for efficient transfer to the FPGA. Metadata is packed separately, allowing the FPGA to quickly access it quickly.

### FPGA Code Implementation

The FPGA processes the prepared partitions in parallel. Each partition is read into the FPGA’s memory, where the data and metadata are parsed. A dynamic scheduler distributes the objects across the available processing elements (PEs). Within each partition, the join units apply a variant of the Nested Loop Join algorithm to check for spatial intersections between objects. The results are collected and written back to the host memory in bursts.

## 6. Dynamic Scheduler

The PBSM implementation on FPGA utilises two different scheduler designs: a static scheduler and a dynamic scheduler.

### Static Scheduler

In the static scheduler, partitions are pre-assigned to specific processing elements (PEs) in a round-robin fashion. Each PE is allocated a fixed number of partitions to process, and the processing is straightforward with minimal overhead.

### Dynamic Scheduler

Similar to the scheduler used in the Indexed Nested Loop Join (INLJ), the dynamic scheduler dynamically assigns partitions to the join units. As each join unit completes its tasks, it reports back to the scheduler, which assigns the next available partition.

