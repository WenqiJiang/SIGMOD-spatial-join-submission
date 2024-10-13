# FPGA Microbenchmarks: Read and Join Experiments

This folder contains scripts to run microbenchmarks for evaluating FPGA-based **read** and **join** units. These scripts execute the benchmarks, collect performance metrics and save the results in JSON format. Two versions of each benchmark are available: **simple** and **meta**, with the meta version introducing additional complexity (e.g., metadata processing or partition-based processing).

## Overview

### 1. Read Benchmark (```read.py```)

The ```read.py``` script runs experiments to benchmark the performance of the **read unit**. It evaluates the unit's cycle-per-page throughput, both in a **simple** scenario (processing only data pages) and in a **meta** scenario (processing both data and metadata pages).

#### Usage

To run the read benchmark:

```
python read.py
```

This script will automatically:
1. Execute the **simple read** microbenchmark using a range of input sizes.
2. Execute the **meta read** microbenchmark with partition and data pages.
3. Save the results in a JSON.

#### Script Details

- **Simple Read**: Measures the performance of the basic read unit by streaming a number of pages (dummy data) into the FPGA.
- **Meta Read**: Evaluates the PBSM read unit, which processes both data and metadata pages.

##### Results

The results are saved in ```micro_read.json``` and include:
- **Input Size**: The number of pages processed.
- **Average Time (ms)**: The average time taken to process the pages.
- **Cycles**: The total number of FPGA clock cycles.
- **Cycles per Page**: The number of cycles required to process each page.

### 2. Join Benchmark (```join.py```)

The ```join.py``` script benchmarks the performance of the **join unit**. The script measures the number of cycles per pair of objects for both the **simple join** (basic nested loop join) and the **meta join** (PBSM join with partition-based processing).

#### Usage

To run the join benchmark:

```
python join.py
```

This script will automatically:
1. Execute the **simple join** microbenchmark using a range of input sizes.
2. Execute the **meta join** microbenchmark with partitioned objects.
3. Save the results in a JSON file.

#### Script Details

- **Simple Join**: Evaluates the performance of the basic join unit by processing pairs of objects.
- **Meta Join**: Evaluates the PBSM join unit, which adds complexity by ensuring valid intersections within partitions.

##### Results

The results are saved in ```micro_join.json``` and include:
- **Input Size**: The number of objects processed.
- **Average Time (ms)**: The average time taken to process the object pairs.
- **Cycles**: The total number of FPGA clock cycles.
- **Cycles per Pair**: The number of cycles required to process each pair of objects.
