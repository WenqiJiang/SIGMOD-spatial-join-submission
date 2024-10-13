# PBSM Experiments

This folder contains scripts and utilities for running and analysing experiments on the PBSM Join algorithm with a dynamic scheduler. The experiments cover various distributions (Uniform, Gaussian, OSM) and test the performance of different numbers of join units.

## 1. Overview

The experiment scripts in this folder allow for automated execution of multiple runs for each configuration, gathering performance metrics and visualising the results. The experiments can be configured to test different datasets and the number of processing units involved in the join operation.

The folder is structured as follows:
- **`collect.py`**: Utility script for running the experiments, gathering results and saving them for further processing.
- **`process.py`**: Script for processing raw experiment results, calculating statistics such as mean, median and confidence intervals.
- **`visualise.py`**: Script for visualising the processed results by plotting graphs that show the performance based on the number of join units.
- **`uniform.py`**, **`gaussian.py`**, **`osm.py`**: Scripts for configuring and running experiments on uniform, Gaussian and OSM datasets, respectively, using  **`collect.py`**.

## 2. Running the Experiments

Each dataset has its own script to run the experiments (`uniform.py`, `gaussian.py`, `osm.py`). To run an experiment, simply execute the corresponding script:

### Example Usage (Uniform Dataset)

```bash
python uniform.py
```

This will execute a set of experiments on the uniform dataset, using the configurations specified in the `uniform.py` file. Similar commands need to be used for Gaussian and OSM datasets.

### Customising the Experiments

You can customise the experiments (e.g. in `uniform.py`)  by modifying the input files and number of processing units:

```python
experiments = [
    {
        "input1": "../../../../data/uniform/txt/uniform_50000_polygon_file_0_set_0.txt",
        "input2": "../../../../data/uniform/txt/uniform_100000_polygon_file_1_set_0.txt",
        "description": "Uniform, 50K x 100K",
        "num_processing_units": [1, 2, 4, 8],
        "additional_args": "60000"
    },
    ...
]
```

- `input1` and `input2`: Paths to the dataset files.
- `description`: A label for the experiment (used in visualisation).
- `num_processing_units`: The numbers of join units to test.
- `additional_args`: Any additional arguments that are passed further to the compiled code (e.g. `60000` here is used for the upper bound for the expected number of join results).

## 3. Processing Results

After running the experiments, use the `process.py` script to process the raw results and calculate statistics (mean, median, standard deviation and confidence intervals):

### Example Usage

```bash
python process.py
```

This will process the results saved from the experiments and prepare them for visualisation.

## 4. Visualising Results

To visualise the processed results, run the `visualise.py` script. This will generate graphs showing how the number of join units impacts the total time for each dataset.

### Example Usage

```bash
python visualise.py
```

This will load the processed results from the `uniform.json`, `gaussian.json` and `osm.json` files and plot the total time for each dataset, based on the number of join units.

## 5. Key Components

### `collect.py`

- Handles running the experiments, collecting output and saving results.
- Extracts metrics such as kernel time and total time.
- Supports multiple initial runs to discard (for initialisation) and actual experiment runs for more accurate results.

### `process.py`

- Loads the raw results and calculates statistical metrics.
- Handles calculation of mean, median, standard deviation and confidence intervals for time metrics.

### `visualise.py`

- Loads processed results and visualises them.
- Plots graphs showing total time, with error bars representing confidence intervals.

## 6. Customising Experiments

You can customise the number of initialisation runs (`P`), actual experiment runs (`N`) and whether to print output from the first initialisation run. These settings can be modified via command-line arguments:

- `--P`: Number of initialisation runs to discard (default: 2).
- `--N`: Number of actual runs to perform (default: 3).
- `--print_first_init`: Print the output of the first initialisation run.

### Example Command

```bash
python uniform.py --P 3 --N 5 --print_first_init
```

This command will run 3 initialisation runs and 5 actual experiment runs, printing the output of the first initialisation run.


## 7. Static vs Dynamic Scheduling Effect

Another experiment compares the performance of the **static** and **dynamic** schedulers in the PBSM algorithm. The dynamic scheduler dynamically assigns partitions to join processing elements based on availability, while the static scheduler pre-assigns partitions in a round-robin fashion. The goal is to measure the impact of dynamic scheduling on overall performance.

### Prerequisites

To run this experiment, you need to have the results for **OSM** and **Uniform** datasets collected and processed as specified earlier in this guide. Make sure to:
1. Run the experiments for both the dynamic and static scheduler versions of the PBSM algorithm.
2. Ensure the results are saved as `osm.json` and `uniform.json` for both static and dynamic runs.

### Running the Experiment

Once the results are collected, you can run the `dynamic_vs_static.py` script to visualise the effect of dynamic vs static scheduling. 

### Usage

```bash
python dynamic_vs_static.py
```
