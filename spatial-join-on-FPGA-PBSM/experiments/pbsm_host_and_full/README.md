# PBSM Host Code Experiments

This folder contains experiments that focus on the performance of the PBSM algorithm’s host code and compare it with the Plane Sweep algorithm. The experiments are divided into two parts:

1. **PBSM Host Code by Part**: This experiment compares the partitioning done by PBSM on the host, subdivided into phases, with the partitioning done by the Plane Sweep algorithm.
2. **PBSM Full Comparison**: This experiment combines the results from the PBSM host code and the PBSM FPGA part (collected from the `pbsm_static` folder) and compares the total result with the Plane Sweep algorithm, which performs both partitioning and joining on the CPU.

## 1. PBSM Host Code by Part

### Overview

This experiment provides a detailed comparison of the partitioning done by PBSM on the host (subdivided into phases such as initial partitioning, refinement and data preparation) against the partitioning done by the Plane Sweep algorithm.

### Phases of PBSM Host Code

The PBSM partitioning process is divided into the following phases:
1. **Grid Preparation**: The first phase where map bounds are computed and grid with cells is initialised.
1. **Initial Partitioning**: The second phase where objects are distributed across grid cells.
2. **Refinement**: Some partitions are further subdivided to ensure they are small enough to be processed by the FPGA.
3. **Data Preparation**: The final step, where partitions are packed into 512-bit pages for the FPGA.

### Collecting Host Partitioning Data

The host partitioning performance data is collected by running `collect.py`. This script will gather results for each phase of the PBSM partitioning and compare them to the Plane Sweep algorithm's partitioning time.

#### Usage

```bash
python collect.py
```

This will collect the partitioning times for both PBSM and Plane Sweep, for different numbers of threads.

### Visualising the Results

After collecting the data, run `process_sweep_vs_pbsm.py` to generate a graph comparing performance between PBSM and Plane Sweep's partitioning.

#### Usage

```bash
python process_sweep_vs_pbsm.py
```

This will create a comparison plot, `sweep_vs_pbsm.png`, showing the performance of PBSM and Plane Sweep partitioning.

## 2. PBSM Full Comparison

### Overview

This experiment measures the total performance of PBSM by combining the performance of its host code and the FPGA part (from `pbsm_static`), and compares it to the performance of the Plane Sweep algorithm, which performs both partitioning and joining on the CPU.

### Prerequisites

Before running this experiment, make sure to:
- Collect results from the FPGA part in the `pbsm_static` folder (refer to the PBSM static scheduler README for more details).
- Collect host code performance data using `collect.py` in this folder.

### Collecting Data for PBSM Host Code

The host code performance data is collected by running `collect.py`, which gathers results for both PBSM and Plane Sweep algorithms.

```bash
python collect.py
```

Once you have the required data, you can run the `process_pbsm_full.py` script to generate comparison plots.

### Usage

```bash
python process_pbsm_full.py
```

This will generate PBSM_full_osm.png and PBSM_full_uniform.png plots.



