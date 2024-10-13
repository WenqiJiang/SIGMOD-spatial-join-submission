import os
import json

from collect import parse_arguments, print_arguments, handle_error, \
    run_experiment, save_data, save_experiment_data

def write_json(fname, overwrite, json_dict, keys, **results):
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
    for key in keys:
        if key not in nested_dict:
            nested_dict[key] = {}
        nested_dict = nested_dict[key]

    # Check if metrics already exist and whether to overwrite them
    if not overwrite and all(metric in nested_dict for metric in results):
        print(f"Metrics for {keys} already exist in the JSON file. Skipping...")
        return

    # Update the metrics in the innermost dictionary
    nested_dict.update(results)

    # Write the updated dictionary to the file
    with open(fname, 'w+') as file:
        print(f"Writing metrics for {keys} to {fname}...")
        json.dump(json_dict, file, indent=2)

def perform_experiments(
        bitstream_directory="../../designs/pbsm/static/1", 
        C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
        output_file="paper_experiments.json",
        datasets=["Uniform", "OSM"],
        join_types=["Polygon-Polygon", "Point-in-Polygon"],
        dataset_sizes=[100000, 1000000, 10000000],
        max_entries=[8, 16, 32],
        num_warmup_runs=1,
        num_runs=3,
        result_allocation_size=int(1e6),
        num_partitions_1d=10, # number of initial partitions per dimension
        overwrite=False,
        ):
        
    print_first_init = True
    
    executable = "host"
    xclbin_file = "xclbin/executor.hw.xclbin"

    print("=" * 50)
    print("Starting experiments...")
    print("=" * 50)
    all_results = {}

    # save the original working directory to return later
    original_cwd = os.getcwd()

    if not os.path.isdir(bitstream_directory):
        handle_error(f"Directory {num_PEs} not found.")

    print("\n" + "=" * 50)
    print(f"Processing num_PEs: {num_PEs}")
    print("=" * 50)
    print()

    # change to the num_PEs directory
    os.chdir(bitstream_directory)
    
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
    
                        additional_args = str(result_allocation_size) + " " + str(num_partitions_1d) + " " + str(max_entry_size ** 2)

                        print("\n" + "-" * 50)
                        print(f"Experiment: {dataset}, {join_type}, {size_dataset_A} x {size_dataset_B}, {max_entry_size} entries")
                        print(f"Inputs: {input1} and {input2}")
                        print("-" * 50)

                        times_collection = {}

                        # print command once before initialization and experiment runs
                        command = f"./{executable} {xclbin_file} {input1} {input2} {additional_args}"
                        print(f"Running experiment with {input1} and {input2} in {bitstream_directory}...")
                        print(command)

                        # discard initialization runs
                        print(f"Initializing with {num_warmup_runs} runs...")
                        for i in range(num_warmup_runs):
                            _ = run_experiment(executable, xclbin_file, input1, input2, additional_args, print_output=(print_first_init and i == 0))

                        times_collection = {}
                        # perform actual experiment runs and collect results
                        print(f"Running {num_runs} experiments...")
                        for i in range(num_runs):
                            print(f"    Run #{i}:")
                            times, _ = run_experiment(executable, xclbin_file, input1, input2, additional_args)

                            for key, value in times.items():
                                if key not in times_collection:
                                    times_collection[key] = []
                                times_collection[key].append(value)

                        dict_keys = [dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size] 
                        times_collection['time_ms'] = times_collection.pop('total_time')
                        times_collection['kernel_time_ms'] = times_collection.pop('kernel_time')
                        write_json(fname=output_file, overwrite=True, json_dict=json_dict, keys=dict_keys, results=times_collection)



    # return to the original working directory after processing the num_PEs
    os.chdir(original_cwd)

    return all_results

if __name__ == "__main__":

    num_PE_list=[2, 4]
    # num_PE_list=[1, 2, 4, 8, 16]
    datasets=["Uniform", "OSM"]
    join_types=["Polygon-Polygon", "Point-in-Polygon"]
    dataset_sizes=[100000, 1000000, 10000000]
    max_entries=[8, 16, 32]
    base_directory="../../designs/pbsm/static"
    overwrite=True

    for num_PEs in num_PE_list:

        bitstream_directory = os.path.join(base_directory, str(num_PEs))

        perform_experiments(
            bitstream_directory=bitstream_directory,
            C_file_dir="/mnt/scratch/wenqi/spatial-join-baseline/generated_data",
            output_file=os.path.join(os.getcwd(), f"paper_experiments_{num_PEs}_PEs.json"),
            datasets=datasets,
            join_types=join_types,
            dataset_sizes=dataset_sizes,
            max_entries=max_entries,
            num_warmup_runs=1,
            num_runs=3,
            result_allocation_size=int(1e6),
            num_partitions_1d=10, # number of initial partitions per dimension
            overwrite=overwrite,
        )
    
