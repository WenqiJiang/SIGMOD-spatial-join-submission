# Nested Loop Join FPGA Implementation

This directory contains the FPGA implementation of the Nested Loop Join (NLJ) algorithm. 

## 1. Introduction

The algorithm works by streaming dataset A sequentially and restreaming dataset B for comparisons, ensuring that every element in A is compared against every element in B.

## 2. Main Features

- **Three Interfaces**: Two input channels for datasets A and B, one output channel for join operation results.
- **Simple Sequential Read**: Dataset A is streamed into the FPGA sequentially.
- **Inner Dataset Restreamed**: Dataset B is re-read and restreamed as needed to compare against every element in A.
- **Static Scheduling**: Fixed scheduling ensures a balanced distribution of work across processing units.
- **Equal Balanced Work Distribution**: Each processing unit gets an equal portion of the workload.
- **Parsers**: Used to decode 512-bit pages (that fully utilise FPGA's memory width) into individual objects for the join operation.
- **Simple Join Unit**: Compares elements from datasets A and B, using static scheduling to ensure equal workload.
- **Collect/Write Unit from SwiftSpatial**: Collects results and writes them back to memory after processing.

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

The implementation expects several command-line arguments to run:

```bash
Usage: ./host xclbin/executor.hw.xclbin <<<data_file_A>>> <<<data_file_B>>> <<<max_num_results>>>
```

- `<<<data_file_A>>>`: Path to the first dataset (Dataset A).
- `<<<data_file_B>>>`: Path to the second dataset (Dataset B).
- `<<<max_num_results>>>`: (Optional) Maximum number of results to collect. Defaults to 1024. 

### Example

```bash
./host xclbin/executor.hw.xclbin ./data/data_A.bin ./data/data_B.bin 2048
```

This command runs the Nested Loop Join algorithm on two datasets (A and B) and allows up to 2048 results to be collected.

## 5. Output

### What a Run Prints

The run will print key metrics for performance evaluation:

- **Kernel Time**: The time taken by the FPGA kernel to execute.
- **Total Time**: The total time from the start to the end of execution, including data transfer, kernel execution and result collection.

```plaintext
...
Kernel time: 1.23 seconds
Total time: 1.45 seconds
```

The output provides an overview of the time spent in kernel execution (on the FPGA) and the total runtime of the operation.