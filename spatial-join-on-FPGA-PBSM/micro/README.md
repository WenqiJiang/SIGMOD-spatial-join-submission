# Microbenchmarks

This directory contains microbenchmarks to evaluate the performance of the read and join units. Each benchmark has two versions: **simple** and **meta**. The simple version tests the unit with basic capabilities, while the meta version introduces additional complexity by incorporating partition metadata.

## Overview

### 1. Read Unit Microbenchmark

The read unit microbenchmarks are designed to evaluate the efficiency of reading data to FPGA from the host. The **simple** version streams dummy data into the read unit while the **meta** version tests the more complex scenario where both metadata and data are streamed through the same interface.

#### Simple Version (read/simple)

This benchmark evaluates the performance of the read unit with a straightforward (dummy) dataset of pages. The goal is to measure the unit’s cycle-per-page throughput.

##### Usage

```
./read_simple <xclbin_path> <num_pages>
```

- `xclbin_path`: Path to the compiled FPGA binary.
- `num_pages`: Number of pages to be generated and processed.

##### Example

```
./read_simple ./fpga_bin.xclbin 1000
```

This command will generate 1000 pages of dummy data and process them using the read unit.

#### Meta Version (read/meta)

This version adds complexity by processing both metadata and data pages, evaluating whether the shared interface between the two affects performance.

##### Usage

```
./read_meta <xclbin_path> <num_pages_meta> <num_pages_data>
```

- `xclbin_path`: Path to the compiled FPGA binary.
- `num_pages_meta`: Number of metadata pages to be processed.
- `num_pages_data`: Number of data pages to be processed.

##### Example

```
./read_meta ./fpga_binary.xclbin 100 1000
```

This will generate 100 metadata pages and 1000 data pages, testing the FPGA-based read unit’s performance when handling both metadata and data streams.

### 2. Join Unit Microbenchmark

The join unit microbenchmarks measure the performance of basic and complex join operations. The **simple** version evaluates the Nested Loop Join unit’s ability to process pairs of objects while the **meta** version tests the Partition-Based Spatial Merge (PBSM) join unit, which adds complexity by checking cell boundaries and ensuring object intersections are valid within partitions.

#### Simple Version (join/simple)

This benchmark tests the basic join unit by streaming pairs of objects into it and measuring the cycle-per-pair throughput.

##### Usage

```
./join_simple <xclbin_path> <num_objects> [intersection_percentage]
```

- `xclbin_path`: Path to the compiled FPGA binary.
- `num_objects`: Number of objects to generate and join.
- `intersection_percentage`: (Optional) Percentage of object intersections. Default is 1.0 (100%).

##### Example

```
./join_simple ./fpga_binary.xclbin 1000
```

This will generate 1000 objects and join them.

#### Meta Version (join/meta)

The meta version introduces the PBSM join unit, which checks additional metadata to ensure valid object intersections within partitions. This complexity slightly affects the performance, but the goal is to assess how well the unit performs despite these checks.

##### Usage

```
./join_meta <xclbin_path> <<<num_partitions>>> <<<num_objs_per_partition>>> <<<intersection_percentage>>> 
```

- `xclbin_path`: Path to the compiled FPGA binary.
- `num_partitions`: (Optional) Number of partitions to generate. Default is 10.
- `num_objs_per_partition`: (Optional) Number of objects per partition. Default is 16.
- `intersection_percentage`: (Optional) Percentage of object intersections. Default is 1.0 (100%).

##### Example

```
./join_meta ./fpga_binary.xclbin 10 16 1.0
```

This will generate 10 partitions, each containing 16 objects, with 100% intersection.