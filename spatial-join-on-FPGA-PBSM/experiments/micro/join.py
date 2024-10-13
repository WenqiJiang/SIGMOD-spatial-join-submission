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
        cycles_per_pair = cycles / (input_size * input_size)

        experiment_data = {
            "input_size": input_size,
            "average_time_ms": average_time,
            "cycles": cycles,
            "cycles_per_pair": cycles_per_pair
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

    for (partition_num, avg_object_in_partition) in inputs:
        print("\n" + "-" * 50)
        print(f"Running experiment with number of partitions: {partition_num}, each with {avg_object_in_partition} objects")
        print("-" * 50)

        average_time = run_experiment(executable, xclbin_file, f"{partition_num} {avg_object_in_partition}")

        cycles = average_time / 0.000005
        cycles_per_pair = cycles / (avg_object_in_partition * avg_object_in_partition * partition_num)

        experiment_data = {
            "P": partition_num,
            "M": avg_object_in_partition,
            "average_time_ms": average_time,
            "cycles": cycles,
            "cycles_per_pair": cycles_per_pair
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


def find_partitions(N, M):
    P = (N // M) ** 2
    return P


if __name__ == "__main__":
    args = parse_arguments()

    input_sizes = [1000, 5000, 10000, 50000]

    # simple join

    base_path = "../../micro/common/join/simple"
    simple_results = perform_experiments(base_path, input_sizes)

    total_average_time_ms = 0.0
    total_cycles = 0.0
    total_cycles_per_pair = 0.0
    num_experiments = len(simple_results)

    for result in simple_results:
        input_size = result["input_size"]
        average_time_ms = result["average_time_ms"]
        cycles = result["cycles"]
        cycles_per_pair = result["cycles_per_pair"]

        total_average_time_ms += average_time_ms
        total_cycles += cycles
        total_cycles_per_pair += cycles_per_pair

    simple_avg_cycles_per_pair = total_cycles_per_pair / num_experiments

    # PBSM join

    chosen_partition_size = 100
    pbsm_inputs = []
    for N in input_sizes:
        P = find_partitions(N, chosen_partition_size)
        pbsm_inputs.append((P, chosen_partition_size))

    base_path = "../../_microbench/common/join/meta_share_inner"
    meta_results = perform_experiments_with_meta(base_path, pbsm_inputs)

    total_average_time_ms = 0.0
    total_cycles = 0.0
    total_cycles_per_pair = 0.0
    num_experiments = len(meta_results)

    for result in meta_results:
        P = result["P"]
        M = result["M"]
        average_time_ms = result["average_time_ms"]
        cycles = result["cycles"]
        cycles_per_pair = result["cycles_per_pair"]

        total_average_time_ms += average_time_ms
        total_cycles += cycles
        total_cycles_per_pair += cycles_per_pair

    meta_avg_cycles_per_pair = total_cycles_per_pair / num_experiments

    # save

    output_file = "micro_join.json"
    save_data({
        "simple": simple_results,
        "meta": meta_results
    }, output_file)

    print("\n" + "=" * 50)
    print(f"Saving results to {output_file}")
    print("=" * 50)

    # print

    print("\n" + "=" * 50)
    print(f"Simple join: averages across all experiments:")
    print(f"  Average Cycles per Pair: {simple_avg_cycles_per_pair}")
    print("=" * 50)

    print("\n" + "=" * 50)
    print(f"PBSM join: averages across all experiments:")
    print(f"  Average Cycles per Pair: {meta_avg_cycles_per_pair}")
    print("=" * 50)