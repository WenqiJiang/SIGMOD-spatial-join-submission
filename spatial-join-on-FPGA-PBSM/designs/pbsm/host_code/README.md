[//]: # (./host ../../../../../data/uniform/txt/uniform_200000_polygon_file_0_set_0.txt ../../../../../data/uniform/txt/uniform_200000_polygon_file_1_set_0.txt 10 2     )

[//]: # ()
[//]: # ()
[//]: # (./host ../../../../../data/osm/txt/OSM_200000_polygon_file_0.txt ../../../../../data/osm/txt/OSM_200000_polygon_file_1.txt 10 2     )

[//]: # ()
[//]: # ()
[//]: # ()
[//]: # (./host ../../../../../data/uniform/txt/uniform_50000_polygon_file_0_set_0.txt ../../../../../data/uniform/txt/uniform_50000_polygon_file_1_set_0.txt 10 2)

[//]: # (./host ../../../../../data/osm/txt/OSM_50000_polygon_file_0.txt ../../../../../data/osm/txt/OSM_50000_polygon_file_1.txt 10 2)




# PBSM and Plane Sweep Host Code

This folder contains the host-side part of the Partition-Based Spatial Merge (PBSM) implementation and Plane Sweep algorithms. The implementations include both sequential and parallel versions of each algorithm, with shared utility functions provided in the `common` folders.

## 1. Introduction

Both algorithms are implemented in two versions:
- **Sequential**: A single-threaded version.
- **Parallel**: A multi-threaded version.

### Partition-Based Spatial Merge (PBSM): Host Code
The host code part of the PBSM divides the datasets into partitions based on a grid structure, refines the partitions and prepares these partitions for FPGA processing.

### Plane Sweep
Plane Sweep is a spatial join algorithm that processes datasets by sweeping through the objects and identifying intersections. The implementation use is the one from https://www.research-collection.ethz.ch/handle/20.500.11850/637149 .

## 2. Main Features

- **Sequential and Parallel Versions**: Both algorithms are implemented in single-threaded and multi-threaded versions.
- **Partitioning with Validation**: PBSM supports validating intermediate results after each partitioning step, ensuring that the partitions and join results are accurate throughout the process.

## 3. Folder Structure

- **pbsm**: The PBSM-specific implementation, with sequential and parallel subfolders.
    - **common**: Contains common utility functions and variables used by both sequential and parallel versions. 
    - **sequential**: The sequential version of PBSM, which processes the data in a single thread.
    - **parallel**: The parallel version of PBSM, which utilizes multi-threading for faster partitioning and joining.
- **sweep**: The Plane Sweep implementation, with sequential and parallel subfolders.
    - **common**: Contains common utility functions and variables used by both sequential and parallel versions.
    - **sequential**: The sequential version of the Plane Sweep algorithm.
    - **parallel**: The parallel version of the Plane Sweep algorithm.

## 4. How to Run the Host Code

### PBSM Sequential

To run the sequential version of PBSM, use the following command in the respective folder:

```
./host <<<data_file_a>>> <<<data_file_b>>> <<<num_partitions_1d>>> <<<max_comparisons_per_partition>>>
```

- `data_file_a`: Path to the first dataset.
- `data_file_b`: Path to the second dataset.
- `num_partitions_1d`: (Optional) The number of initial partitions along one dimension. Defaults to 10.
- `max_comparisons_per_partition`: (Optional) The maximum number of comparisons allowed per partition (i.e. |A| * |B| <= `max_comparisons_per_partition`). Defaults to 1000.

### PBSM Parallel

To run the parallel version of PBSM, use the following command:

```bash
./host <<<data_file_a>>> <<<data_file_b>>> <<<num_partitions_1d>>> <<<max_comparisons_per_partition>>> <<<num_threads>>>
```

- `data_file_a`: Path to the first dataset.
- `data_file_b`: Path to the second dataset.
- `num_partitions_1d`: (Optional) The number of initial partitions along one dimension. Defaults to 10.
- `max_comparisons_per_partition`: (Optional) The maximum number of comparisons allowed per partition (i.e. |A| * |B| <= `max_comparisons_per_partition`). Defaults to 1000.
- `num_threads`: (Optional) The number of threads for parallel processing. Defaults to 1.

### Plane Sweep Sequential

To run the sequential version of the Plane Sweep algorithm, use the following command:

```bash
./host <<<file_name_1>>> <<<file_name_2>>>
```

- `file_name_1`: Path to the first dataset.
- `file_name_2`: Path to the second dataset.

### Plane Sweep Parallel

To run the parallel version of the Plane Sweep algorithm, use the following command:

```bash
./host <<<file_name_1>>> <<<file_name_2>>> <<<num_partitions_1d>>> <<<num_threads>>>
```

- `file_name_1`: Path to the first dataset.
- `file_name_2`: Path to the second dataset.
- `num_partitions_1d`: (Optional) The number of partitions along one dimension. Defaults to 10.
- `num_threads`: (Optional) The number of threads for parallel processing. Defaults to 1.

### Example

Here is an example of running the PBSM parallel version:

```bash
./host ../../../../../data/uniform/txt/uniform_200000_polygon_file_0_set_0.txt ../../../../../data/uniform/txt/uniform_200000_polygon_file_1_set_0.txt 10 1000 2
```

This runs the PBSM algorithm on two datasets, dividing the domain into 10 initial partitions along one dimension, allowing up to 1000 comparisons within a partition and using 2 threads.

## 5. Partitioning Validation

PBSM implementation supports an argument called `validate_intermediate`. This option is used to run the C++ join algorithm after each partitioning step to verify that the results are correct at each stage. To enable this, add the `validate_intermediate` flag when calling the `host_partition` function.

```cpp
host_partition(dataset_a, dataset_b, num_partitions_1d, max_comparisons_per_partition, num_threads, true);
```