import subprocess
import re
import json
import numpy as np
import os

from collect import compile_cpp, extract_times_from_output

def write_json(fname, overwrite, json_dict, keys, results):
    """
    Writes the json dictionary, updating it with the given keys and metrics.

    Parameters:
    - fname (str): The filename to write the JSON data to.
    - overwrite (bool): Whether to overwrite existing metric values or not.
    - json_dict (dict): The dictionary to update and write to the file.
    - keys (list): A list of keys to form the nested structure in the dictionary.
    - metrics (dict): Key-value pairs to update the innermost dictionary with.
    
    Note: All keys are stored as strings in the JSON.
    """
    # Convert all keys to string format
    keys = list(map(str, keys))

    # Create the nested structure in the JSON dictionary
    nested_dict = json_dict
    for i, key in enumerate(keys):
        if key not in nested_dict:
            nested_dict[key] = {}
        nested_dict = nested_dict[key]

    if not overwrite and all(metric in nested_dict for metric in results):
        print(f"Metrics for {keys} already exist in the JSON file. Skipping...")
        return

    # Update the metrics in the innermost dictionary
    nested_dict.update(results)

    # Write the updated dictionary to the file
    with open(fname, 'w+') as file:
        print(f"Writing metrics for {keys} to {fname}...")
        json.dump(json_dict, file, indent=2)


def run_cpp_fpga_host(executable_name, data_file_a, data_file_b, num_partitions_1d, max_comparisons_per_partition, num_threads=None):
    run_command = [f'./{executable_name}', data_file_a, data_file_b]

    run_command.append(str(num_partitions_1d))
    run_command.append(str(max_comparisons_per_partition))

    if num_threads is not None:
        run_command.append(str(num_threads))

    print("Exeucuting command: ", run_command)
    result = subprocess.run(run_command, capture_output=True, text=True)

    # extract timing info
    times = extract_times_from_output(result.stdout)
    return times


def run_cpp_cpu_baseline(executable_name, data_file_a, data_file_b, num_partitions_1d, num_threads=None):
    run_command = [f'./{executable_name}', data_file_a, data_file_b]

    run_command.append(str(num_partitions_1d))

    if num_threads is not None:
        run_command.append(str(num_threads))

    print("Exeucuting command: ", run_command)
    result = subprocess.run(run_command, capture_output=True, text=True)

    # extract timing info
    times = extract_times_from_output(result.stdout)
    return times

def perform_experiments_fpga_host(
        cpp_dir = "../../designs/pbsm/host_code/pbsm/parallel",
        executable_name='host',
        C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
        output_file="paper_experiments.json",
        datasets=["Uniform", "OSM"],
        join_types=["Polygon-Polygon", "Point-in-Polygon"],
        dataset_sizes=[100000, 1000000, 10000000],
        max_entries=[8, 16, 32],
        num_warmup_runs=1,
        num_runs=5,
        num_partitions_1d=10, # number of initial partitions per dimension
        num_threads=16,
        overwrite=False,
        ):

    original_cwd = os.getcwd()
    os.chdir(cpp_dir)
    compile_cpp(cpp_dir)

    print(os.getcwd())

    # init json
    json_dict = {}

    for dataset in datasets:
        for join_type in join_types:
            for size_dataset_A in dataset_sizes:
                for size_dataset_B in dataset_sizes:

                    # "Point-in-Polygon": Point File 0 (size_dataset_A), Polygon File 0 (size_dataset_B)
                    # "Polygon-Polygon": Polygon File 0 (size_dataset_A), Polygon File 1 (size_dataset_B)
                    if dataset == 'Uniform':
                        if join_type == "Polygon-Polygon":
                            file_A = f"C_uniform_{size_dataset_A}_polygon_file_0_set_0.txt"
                            file_B = f"C_uniform_{size_dataset_B}_polygon_file_1_set_0.txt"
                        elif join_type == "Point-in-Polygon":
                            file_A = f"C_uniform_{size_dataset_A}_point_file_0.txt"
                            file_B = f"C_uniform_{size_dataset_B}_polygon_file_0_set_0.txt"
                    elif dataset == "OSM":
                        if join_type == "Polygon-Polygon":
                            file_A = f"C_OSM_{size_dataset_A}_polygon_file_0.txt"
                            file_B = f"C_OSM_{size_dataset_B}_polygon_file_1.txt"
                        elif join_type == "Point-in-Polygon":
                            file_A = f"C_OSM_{size_dataset_A}_point_file_0.txt"
                            file_B = f"C_OSM_{size_dataset_B}_polygon_file_0.txt"

                    input1 = os.path.join(C_file_dir, file_A)
                    input2 = os.path.join(C_file_dir, file_B)

                    for max_entry_size in max_entries:
   
                        print("\n" + "-" * 50)
                        print(f"Experiment: {dataset}, {join_type}, {size_dataset_A} x {size_dataset_B}, {max_entry_size} entries")
                        print(f"Inputs: {input1} and {input2}")
                        print("-" * 50) 

                        max_comparisons_per_partition = max_entry_size ** 2
                        # warm up
                        print(f"Initializing with {num_warmup_runs} runs...")
                        for _ in range(num_warmup_runs):
                            run_cpp_fpga_host(executable_name, input1, input2, num_partitions_1d, max_comparisons_per_partition, num_threads)

                        # measure
                        print(f"Running {num_runs} experiments...")
                        collected_times = []
                        for i in range(num_runs):
                            print(f"    Run {i}.")
                            times = run_cpp_fpga_host(executable_name, input1, input2, num_partitions_1d, max_comparisons_per_partition, num_threads)
                            collected_times.append(times)


                        run_results = {
                            'threads': num_threads,
                            'num_partitions_1d': num_partitions_1d,
                            'max_comparisons_per_partition': max_comparisons_per_partition,
                        }
                        times_key = collected_times[0].keys()

                        for collected_time in collected_times:
                            for key in times_key:
                                if key not in run_results:
                                    run_results[key] = []
                                run_results[key].append(collected_time[key])
                        
                        print("run_results: ", run_results)
                        dict_keys = [dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size] 
                        write_json(fname=output_file, overwrite=True, json_dict=json_dict, 
                                   keys=dict_keys, results=run_results)
                    
    # return to the original working directory after processing the num_PEs
    os.chdir(original_cwd)


def perform_experiments_cpu_baseline(
        cpp_dir = "../../designs/pbsm/host_code/pbsm/parallel",
        executable_name='host',
        C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
        output_file="paper_experiments.json",
        datasets=["Uniform", "OSM"],
        join_types=["Polygon-Polygon", "Point-in-Polygon"],
        dataset_sizes=[100000, 1000000, 10000000],
        num_warmup_runs=1,
        num_runs=5,
        num_partitions_1d_list=[10, 100], # number of initial partitions per dimension
        num_threads=16,
        overwrite=False,
        ):

    original_cwd = os.getcwd()
    os.chdir(cpp_dir)
    compile_cpp(cpp_dir)

    print(os.getcwd())

    # init json
    json_dict = {}

    for dataset in datasets:
        for join_type in join_types:
            for size_dataset_A in dataset_sizes:
                for size_dataset_B in dataset_sizes:

                    # "Point-in-Polygon": Point File 0 (size_dataset_A), Polygon File 0 (size_dataset_B)
                    # "Polygon-Polygon": Polygon File 0 (size_dataset_A), Polygon File 1 (size_dataset_B)
                    if dataset == 'Uniform':
                        if join_type == "Polygon-Polygon":
                            file_A = f"C_uniform_{size_dataset_A}_polygon_file_0_set_0.txt"
                            file_B = f"C_uniform_{size_dataset_B}_polygon_file_1_set_0.txt"
                        elif join_type == "Point-in-Polygon":
                            file_A = f"C_uniform_{size_dataset_A}_point_file_0.txt"
                            file_B = f"C_uniform_{size_dataset_B}_polygon_file_0_set_0.txt"
                    elif dataset == "OSM":
                        if join_type == "Polygon-Polygon":
                            file_A = f"C_OSM_{size_dataset_A}_polygon_file_0.txt"
                            file_B = f"C_OSM_{size_dataset_B}_polygon_file_1.txt"
                        elif join_type == "Point-in-Polygon":
                            file_A = f"C_OSM_{size_dataset_A}_point_file_0.txt"
                            file_B = f"C_OSM_{size_dataset_B}_polygon_file_0.txt"

                    input1 = os.path.join(C_file_dir, file_A)
                    input2 = os.path.join(C_file_dir, file_B)

                    for num_partitions_1d in num_partitions_1d_list:
   
                        num_partitions_total = num_partitions_1d ** 2
                        print("\n" + "-" * 50)
                        print(f"Experiment: {dataset}, {join_type}, {size_dataset_A} x {size_dataset_B}, {num_partitions_total} partitions ({num_partitions_1d} per dimension)")
                        print(f"Inputs: {input1} and {input2}")
                        print("-" * 50) 

                        # warm up
                        print(f"Initializing with {num_warmup_runs} runs...")
                        for _ in range(num_warmup_runs):
                            run_cpp_cpu_baseline(executable_name, input1, input2, num_partitions_1d, num_threads)

                        # measure
                        print(f"Running {num_runs} experiments...")
                        collected_times = []
                        for i in range(num_runs):
                            print(f"    Run {i}.")
                            times = run_cpp_cpu_baseline(executable_name, input1, input2, num_partitions_1d, num_threads)
                            collected_times.append(times)


                        run_results = {
                            'threads': num_threads,
                            'num_partitions_1d': num_partitions_1d,
                            'num_partitions_total': num_partitions_total, 
                            'max_comparisons_per_partition': None,
                        }
                        times_key = collected_times[0].keys()

                        for collected_time in collected_times:
                            for key in times_key:
                                if key not in run_results:
                                    run_results[key] = []
                                run_results[key].append(collected_time[key])
                        
                        print("run_results: ", run_results)
                        dict_keys = [dataset, join_type, size_dataset_A, size_dataset_B, num_partitions_total] 
                        write_json(fname=output_file, overwrite=True, json_dict=json_dict, 
                                   keys=dict_keys, results=run_results)
                    
    # return to the original working directory after processing the num_PEs
    os.chdir(original_cwd)

if __name__ == '__main__':

    num_threads_list = [16, 1]
    # num_threads_list = [1, 2, 4, 8, 16]
    modes = ['cpu_baseline']
    # modes = ['fpga_host', 'cpu_baseline']
    # datasets=["Uniform"]
    datasets=["Uniform", "OSM"]
    join_types=["Polygon-Polygon", "Point-in-Polygon"]
    dataset_sizes=[100000, 1000000, 10000000]
    max_entries=[8, 16, 32]
    base_directory="../../designs/pbsm/static"
    overwrite=True
    num_partitions_1d_cpu = [10, 100]

    for num_threads in num_threads_list:
        for mode in modes:
            if mode == 'cpu_baseline':
                cpp_dir = "../../designs/pbsm/host_code/sweep/parallel"
                perform_experiments_cpu_baseline(
                    cpp_dir=cpp_dir,
                    executable_name='host',
                    C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
                    output_file=os.path.join(os.getcwd(), f"paper_experiments_{mode}_{num_threads}_threads.json"),
                    datasets=datasets,
                    join_types=join_types,
                    dataset_sizes=dataset_sizes,
                    num_warmup_runs=1,
                    num_runs=5,
                    num_partitions_1d_list=num_partitions_1d_cpu,
                    num_threads=num_threads,
                    overwrite=overwrite,
                )
            elif mode == 'fpga_host':
                cpp_dir = "../../designs/pbsm/host_code/pbsm/parallel"
                perform_experiments_fpga_host(
                    cpp_dir=cpp_dir,
                    executable_name='host',
                    C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
                    output_file=os.path.join(os.getcwd(), f"paper_experiments_{mode}_{num_threads}_threads.json"),
                    datasets=datasets,
                    join_types=join_types,
                    dataset_sizes=dataset_sizes,
                    max_entries=max_entries,
                    num_warmup_runs=1,
                    num_runs=5,
                    num_partitions_1d=10, # number of initial partitions per dimension
                    num_threads=num_threads,
                    overwrite=overwrite,
                )
            else:
                raise ValueError(f"Invalid mode: {mode}")
