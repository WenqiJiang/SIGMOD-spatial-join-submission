import os
import subprocess
import json
import argparse
from utils import handle_error, extract_average_time_from_csv


def run_experiment(executable, xclbin, params):
    command = f"./{executable} {xclbin} {params}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(result.stdout)

    summary_csv = "summary.csv"
    if os.path.exists(summary_csv):
        time = extract_average_time_from_csv(summary_csv)
    else:
        time = None
        handle_error("summary.csv not found.")

    return time


def perform_experiments(base_directory, input_sizes):
    executable = "host"
    xclbin_file = "xclbin/executor.hw.xclbin"

    print("=" * 50)
    print("Starting experiments...")
    print("=" * 50)
    all_results = []

    original_cwd = os.getcwd()

    os.chdir(base_directory)

    for input_size in input_sizes:
        print("\n" + "-" * 50)
        print(f"Running experiment with input size: {input_size}")
        print("-" * 50)

        average_time = run_experiment(executable, xclbin_file, input_size)

        cycles = average_time / 0.000005
        cycles_per_pair = cycles / input_size

        experiment_data = {
            "input_size": input_size,
            "average_time_ms": average_time,
            "cycles": cycles,
            "cycles_per_page": cycles_per_pair
        }

        all_results.append(experiment_data)

    os.chdir(original_cwd)

    print("=" * 50)
    print("Experiments completed!")
    print("=" * 50)

    return all_results


def perform_experiments_with_meta(base_directory, inputs):
    executable = "host"
    xclbin_file = "xclbin/executor.hw.xclbin"

    print("=" * 50)
    print("Starting experiments...")
    print("=" * 50)
    all_results = []

    original_cwd = os.getcwd()

    os.chdir(base_directory)

    for (partition_pages, data_pages) in inputs:
        print("\n" + "-" * 50)
        print(f"Running experiment with total {partition_pages} partition pages and {data_pages} data pages")
        print("-" * 50)

        average_time = run_experiment(executable, xclbin_file, f"{partition_pages} {data_pages}")

        cycles = average_time / 0.000005
        cycles_per_pair = cycles / (partition_pages + data_pages)

        experiment_data = {
            "P": partition_pages,
            "D": data_pages,
            "average_time_ms": average_time,
            "cycles": cycles,
            "cycles_per_page": cycles_per_pair
        }

        all_results.append(experiment_data)

    os.chdir(original_cwd)

    print("=" * 50)
    print("Experiments completed!")
    print("=" * 50)

    return all_results


def save_data(data, output_file):
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run experiments and analyse results.')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    input_sizes = [1000, 5000, 10000, 50000]

    # simple read

    base_path = "../../micro/common/read/simple"
    simple_results = perform_experiments(base_path, input_sizes)

    total_average_time_ms = 0.0
    total_cycles = 0.0
    total_cycles_per_page = 0.0
    num_experiments = len(simple_results)

    for result in simple_results:
        input_size = result["input_size"]
        average_time_ms = result["average_time_ms"]
        cycles = result["cycles"]
        cycles_per_page = result["cycles_per_page"]

        total_average_time_ms += average_time_ms
        total_cycles += cycles
        total_cycles_per_page += cycles_per_page

    simple_avg_cycles_per_page = total_cycles_per_page / num_experiments

    # PBSM read

    chosen_partition_size = 100 // 3
    pbsm_inputs = []
    for N in input_sizes:
        P = N // chosen_partition_size
        pbsm_inputs.append((P // 5, N))

    base_path = "../../_microbench/common/read/meta"
    meta_results = perform_experiments_with_meta(base_path, pbsm_inputs)

    total_average_time_ms = 0.0
    total_cycles = 0.0
    total_cycles_per_page = 0.0
    num_experiments = len(meta_results)

    for result in meta_results:
        average_time_ms = result["average_time_ms"]
        cycles = result["cycles"]
        cycles_per_page = result["cycles_per_page"]

        total_average_time_ms += average_time_ms
        total_cycles += cycles
        total_cycles_per_page += cycles_per_page

    meta_avg_cycles_per_page = total_cycles_per_page / num_experiments

    # save

    output_file = "micro_read.json"
    save_data({
        "simple": simple_results,
        "meta": meta_results
    }, output_file)

    print("\n" + "=" * 50)
    print(f"Saving results to {output_file}")
    print("=" * 50)

    # print

    print("\n" + "=" * 50)
    print(f"Simple read: averages across all experiments:")
    print(f"  Average cycles per page (512 bits): {simple_avg_cycles_per_page}")
    print("=" * 50)

    print("\n" + "=" * 50)
    print(f"PBSM (meta) read: averages across all experiments:")
    print(f"  Average cycles per page (512 bits): {meta_avg_cycles_per_page}")
    print("=" * 50)