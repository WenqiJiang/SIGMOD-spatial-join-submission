import os
import subprocess
import json
import csv
import argparse
import re
import sys


def handle_error(error, output=None):
    print(f"Error: {error}")
    print()

    if output is not None:
        print("Program output:")
        print()

        for line in output.splitlines():
            print(line)

    sys.exit(1)


def extract_average_time_from_csv(csv_file):
    # open the csv file and locate the relevant section
    with open(csv_file, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            if "Compute Unit Utilization" in row:
                break

        # skip the header and extract 'Average Time (ms)'
        next(reader)
        for row in reader:
            if len(row) > 11:
                return float(row[11])

    handle_error(f"Could not extract average time from {csv_file}.")


def extract_times_from_output(output):
    times = {}
    results = None

    time_pattern = re.compile(r"TIME\[(\w+)](?:\s*\(.*?\))?\s*:\s*([\d.]+)\s*ms\.?")
    results_pattern = re.compile(r"RESULTS:\s*(\d+)")

    for line in output.splitlines():
        time_match = time_pattern.search(line)
        results_match = results_pattern.search(line)

        if time_match:
            time_name = time_match.group(1)
            time_value = float(time_match.group(2))
            times[time_name] = time_value

        if results_match:
            results = int(results_match.group(1))

    if not times:
        handle_error("No time metrics found in program output.", output)

    if results is None:
        handle_error("No results found in program output.", output)

    return times, results


def run_experiment(executable, xclbin, input1, input2, additional_args="", print_output=False):
    command = f"./{executable} {xclbin} {input1} {input2} {additional_args}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if print_output:
        print("Output of the first initialization run:")
        print("\n" + "." * 50)
        print(result.stdout)
        print("\n" + "." * 50)

    times, results = extract_times_from_output(result.stdout)

    summary_csv = "summary.csv"
    if os.path.exists(summary_csv):
        times['compute_unit_avg_time'] = extract_average_time_from_csv(summary_csv)
    else:
        handle_error("summary.csv not found.")

    return times, results


def perform_experiments(base_directory, experiments, output_file, args, save_to_file=True):
    P = args.P
    N = args.N
    print_first_init = args.print_first_init

    executable = "host"
    xclbin_file = "xclbin/executor.hw.xclbin"

    print("=" * 50)
    print("Starting experiments...")
    print("=" * 50)
    all_results = {}

    # save the original working directory to return later
    original_cwd = os.getcwd()

    # extract all unique variants from the experiments
    variants = sorted({variant for experiment in experiments for variant in experiment["num_processing_units"]})

    # iterate over each variant, and perform all relevant experiments for this variant
    for variant in variants:
        version_path = os.path.join(base_directory, str(variant))

        if not os.path.isdir(version_path):
            handle_error(f"Directory {variant} not found.")

        print("\n" + "=" * 50)
        print(f"Processing variant: {variant}")
        print("=" * 50)
        print()

        os.chdir(version_path)  # change to the variant directory

        for experiment in experiments:
            if variant not in experiment["num_processing_units"]:
                continue  # skip experiments that don't include this variant

            description = experiment["description"]
            input1 = experiment["input1"]
            input2 = experiment["input2"]
            additional_args = experiment.get("additional_args_per_variant", {}).get(variant, experiment.get("additional_args", ""))

            print("\n" + "-" * 50)
            print(f"Experiment: {description}")
            print(f"Inputs: {input1} and {input2}")
            print("-" * 50)

            if description not in all_results:
                all_results[description] = []

            times_collection = {}

            # print command once before initialization and experiment runs
            command = f"./{executable} {xclbin_file} {input1} {input2} {additional_args}"
            print(f"Running experiment with {input1} and {input2} in {version_path}...")
            print(command)

            # discard initialization runs
            print(f"Initializing with {P} runs...")
            for i in range(P):
                _ = run_experiment(executable, xclbin_file, input1, input2, additional_args, print_output=(print_first_init and i == 0))

            # perform actual experiment runs and collect results
            print(f"Running {N} experiments...")
            for i in range(N):
                print(f"    Run #{i}:")
                times, _ = run_experiment(executable, xclbin_file, input1, input2, additional_args)

                for key, value in times.items():
                    if key not in times_collection:
                        times_collection[key] = []
                    times_collection[key].append(value)

            experiment_data = {
                "version": variant,
                "input1": input1,
                "input2": input2,
                "additional_args": additional_args,
                "times": times_collection
            }

            all_results[description].append(experiment_data)

            if save_to_file:
                save_experiment_data(experiment_data, base_directory, description, variant)

        # return to the original working directory after processing the variant
        os.chdir(original_cwd)

    if save_to_file:
        print("\n" + "=" * 50)
        print(f"Saving results to {output_file}")
        print("=" * 50)
        save_data(all_results, output_file)

    print("=" * 50)
    print("Experiments completed!")
    print("=" * 50)

    return all_results


def save_data(data, output_file):
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)


def save_experiment_data(experiment_data, base_directory, description, variant):
    safe_description = re.sub(r'[^\w\s-]', '', description).replace(' ', '_')
    file_name = f"{safe_description}_{variant}.json"
    # file_path = os.path.join(base_directory, file_name)
    # file_path = './'

    with open(file_name, 'w') as f:
        json.dump(experiment_data, f, indent=4)

    print(f"Data for experiment '{description}' (variant: {variant}) saved to {file_name}.")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run experiments and analyse results.')
    parser.add_argument('--P', type=int, default=2, help='Number of initialisation runs to discard')
    parser.add_argument('--N', type=int, default=3, help='Number of actual runs to perform')
    parser.add_argument('--print_first_init', action='store_true', help='Print the output of the first initialization run')
    return parser.parse_args()


def print_arguments(args, experiments, base_path, output_file):
    print("Starting script with the following parameters:")
    print(f"  Base path: {base_path}")
    print(f"  P: {args.P}")
    print(f"  N: {args.N}")
    print(f"  Output file: {output_file}")
    print(f"  Print first initialization run: {args.print_first_init}")
    print(f"  Experiments:")
    for experiment in experiments:
        print(f"    {experiment['description']}")
