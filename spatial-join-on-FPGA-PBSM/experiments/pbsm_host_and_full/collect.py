import subprocess
import re
import json
import numpy as np
import os


def load_existing_data(json_file):
    if os.path.exists(json_file):
        with open(json_file, 'r') as file:
            return json.load(file)
    return {}


def save_data(json_file, data):
    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)


def check_experiment_exists(data, key, threads):
    if key in data:
        for run in data[key]:
            if run['threads'] == threads:
                return True
    return False


def clean_cpp(folder):
    compile_command = ['make', 'clean']
    subprocess.run(compile_command, check=True)


def compile_cpp(folder):
    clean_cpp(folder)
    compile_command = ['make']
    subprocess.run(compile_command, check=True)


def extract_times_from_output(output):
    times = {}
    time_pattern = re.compile(r"TIME\[(\w+)](?:\s*\(.*?\))?\s*:\s*([\d.]+)\s*ms\.?")

    for line in output.splitlines():
        time_match = time_pattern.search(line)
        if time_match:
            time_name = time_match.group(1)
            time_value = float(time_match.group(2))
            times[time_name] = time_value

    if not times:
        print(output)
        raise ValueError("No time metrics found in program output!")

    return times


def run_cpp(folder, executable_name, data_file_a, data_file_b, num_partitions_1d, max_comparisons_per_partition, num_threads=None):
    run_command = [f'./{executable_name}', data_file_a, data_file_b]

    run_command.append(str(num_partitions_1d))
    run_command.append(str(max_comparisons_per_partition))

    if num_threads is not None:
        run_command.append(str(num_threads))

    result = subprocess.run(run_command, capture_output=True, text=True)

    # extract timing info
    times = extract_times_from_output(result.stdout)
    return times


def collect_data(base_directory, experiments, thread_counts, data_file_a, data_file_b, P_initial, P_times, json_file, override_existing=False):
    if override_existing:
        data = {}
    else:
        data = load_existing_data(json_file)

    original_cwd = os.getcwd()

    print("\n" + "-" * 50)
    print(f"Inputs: {data_file_a} and {data_file_b}")
    print("-" * 50)

    for experiment in experiments:
        folder = experiment['folder']
        executable_name = experiment['executable']
        num_partitions_1d = experiment.get('num_partitions_1d')
        max_comparisons_per_partition = experiment.get('max_comparisons_per_partition')

        path = os.path.join(base_directory, folder)
        os.chdir(path)

        print(os.getcwd())

        if folder not in data:
            data[folder] = []

        print("\n" + "-" * 50)
        print(f"Experiment: {folder}")
        print("-" * 50)

        compile_cpp(path)

        for threads in thread_counts:
            print(f"Running experiment with {threads} threads...")

            if check_experiment_exists(data, folder, threads):
                print(f"Skipping {folder} with {threads} threads (already exists).")
                continue

            # warm up
            print(f"Initializing with {P_initial} runs...")
            for _ in range(P_initial):
                run_cpp(folder, executable_name, data_file_a, data_file_b, num_partitions_1d, max_comparisons_per_partition, threads)

            # measure
            print(f"Running {P_times} experiments...")
            collected_times = []
            for i in range(P_times):
                print(f"    Run {i}.")
                times = run_cpp(folder, executable_name, data_file_a, data_file_b, num_partitions_1d, max_comparisons_per_partition, threads)
                collected_times.append(times)

            run_results = {
                'threads': threads,
                'executable': executable_name,
                'num_partitions_1d': num_partitions_1d,
                'max_comparisons_per_partition': max_comparisons_per_partition,
                'runs': collected_times,
                'average': {},
                'std_dev': {},
                'std_err': {}
            }

            for time in collected_times[0]:
                values = np.array([run[time] for run in collected_times])
                run_results['average'][time] = np.mean(values)
                run_results['std_dev'][time] = np.std(values)
                run_results['std_err'][time] = np.std(values) / np.sqrt(P_times)

            data[folder].append(run_results)

        os.chdir(original_cwd)

    print("\n" + "=" * 50)
    print(f"Saving results to {json_file}")
    print("=" * 50)
    save_data(json_file, data)

    print("=" * 50)
    print("Experiments completed!")
    print("=" * 50)

    return data


def prepare_and_run_experiment(file_name_a, file_name_b, folder):
    data_file_a = f'../../../../../data/{folder}/txt/{file_name_a}.txt'
    data_file_b = f'../../../../../data/{folder}/txt/{file_name_b}.txt'

    thread_counts = [1, 2, 4, 8, 16]
    P = 5
    N = 10
    base_directory = "../../designs/pbsm/host_code"

    # list of experiments
    experiments = [
        {
            'folder': 'pbsm/parallel',
            'executable': 'host',
            'num_partitions_1d': 10,
            'max_comparisons_per_partition': 1000,
        },
        {
            'folder': 'pbsm/sequential',
            'executable': 'host',
            'num_partitions_1d': 10,
            'max_comparisons_per_partition': 1000,
        },
        {
            'folder': 'sweep/parallel',
            'executable': 'host',
            'num_partitions_1d': 10,
            'max_comparisons_per_partition': 1000,
        },
    ]

    # gen file name
    results_file_name = f'results__{file_name_a}__{file_name_b}__{P}_{N}.json'

    collect_data(base_directory, experiments, thread_counts, data_file_a, data_file_b, P, N, results_file_name, override_existing=True)


if __name__ == '__main__':
    prepare_and_run_experiment(
        file_name_a='uniform_100000_polygon_file_0_set_0',
        file_name_b='uniform_100000_polygon_file_1_set_0',
        folder='uniform'
    )

    prepare_and_run_experiment(
        file_name_a='uniform_200000_polygon_file_0_set_0',
        file_name_b='uniform_200000_polygon_file_1_set_0',
        folder='uniform'
    )

    prepare_and_run_experiment(
        file_name_a='OSM_100000_polygon_file_0',
        file_name_b='OSM_100000_polygon_file_1',
        folder='osm'
    )

    prepare_and_run_experiment(
        file_name_a='OSM_200000_polygon_file_0',
        file_name_b='OSM_200000_polygon_file_1',
        folder='osm'
    )
