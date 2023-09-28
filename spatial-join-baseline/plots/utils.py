import json
import numpy as np

def load_json(fname):
    with open(fname, 'r') as f:
        json_dict = json.load(f)
    return json_dict
    
def load_FPGA_json(fname):
    """
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B][max_entry_size]
    perf_set = {num_results: ..., time_ms: [], kernel_time_ms: []} -> size of the time array == number of runs

    dataset = ("Uniform", "OSM")
    join type = ("Point-in-Polygon", "Polygon-Polygon")
    size_dataset_A, size_dataset_B: int, e.g., 100000
    max_entry_size: int, e.g., 16

    num_results: int, e.g., 32141
    time_ms: array of float, in ms
    kernel_time_ms: array of float, in ms

    "Point-in-Polygon": Point File 0 (size_dataset_A), Polygon File 0 (size_dataset_B)
    "Polygon-Polygon": Polygon File 0 (size_dataset_A), Polygon File 1 (size_dataset_B)
    """
    return load_json(fname)

def load_cpp_json(fname):
    """
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B][max_entry_size]
    perf_set = {num_results: ..., build_index_1_ms: [], build_index_2_ms: [], bfs_dynamic_ms: []} -> size of the time array == number of runs
    """
    return load_json(fname)

def load_postgis_json(fname):
    """
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B]
    perf_set = {num_results: ..., build_index_1_ms: [], build_index_2_ms: [], join_time_ms: []} -> size of the time array == number of runs
    """
    return load_json(fname)

def load_cuspatial_json(fname):
    """
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B]
    perf_set = {num_results: ..., build_index_time_ms: , join_time_ms: []} -> size of the time array == number of runs
    """
    return load_json(fname)

def load_sedona_json(fname):
    """
    TODO: add partitoin time in sedona
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B]
    perf_set = {num_results: ..., build_index_ms: [], partition_1_time_ms: [], partition_2_time_ms: [], join_time_ms: []} -> size of the time array == number of runs
    """
    return load_json(fname)

def load_spatialspark_json(fname):
    """
    TODO: add partitoin time in spatial spark
    Output Json format:
    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B]
    perf_set = {num_results: ..., build_index_1_ms: [], build_index_2_ms: [], partition_1_time_ms: [], partition_2_time_ms: [], join_time_ms: []} -> size of the time array == number of runs
    """
    return load_json(fname)

def compare_cpp_FPGA_num_results(fname_cpp, fname_FPGA):
    json_dict_cpp = load_cpp_json(fname_cpp)
    json_dict_FPGA = load_FPGA_json(fname_FPGA)
    for dataset in json_dict_FPGA:
        for join_type in json_dict_FPGA[dataset]:
            for size_dataset_A in json_dict_FPGA[dataset][join_type]:
                for size_dataset_B in json_dict_FPGA[dataset][join_type][size_dataset_A]:
                    for max_entry_size in json_dict_FPGA[dataset][join_type][size_dataset_A][size_dataset_B]:
                        assert json_dict_FPGA[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]['num_results'] == \
                            json_dict_cpp[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]['num_results']
                        
    print("cpp and FPGA results consistent")


def cp_perf_dict_format(json_dict, contain_max_entry=True):
    """
    Given an input dict, create a one with the same keys, with None as values
        d[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B][max_entry_size] = None
    """
    empty_dict = dict()
    for dataset in json_dict:
        if dataset not in empty_dict:
            empty_dict[dataset] = dict()

        for join_type in json_dict[dataset]:
            if join_type not in empty_dict[dataset]:
                empty_dict[dataset][join_type] = dict()

            for size_dataset_A in json_dict[dataset][join_type]:
                if size_dataset_A not in empty_dict[dataset][join_type]:
                    empty_dict[dataset][join_type][size_dataset_A] = dict()

                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:
                    if size_dataset_B not in empty_dict[dataset][join_type][size_dataset_A]:
                        empty_dict[dataset][join_type][size_dataset_A][size_dataset_B] = dict()

                    if contain_max_entry:
                        for max_entry_size in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            if max_entry_size not in empty_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                                empty_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] = dict()
    return empty_dict

def cp_cpp_dict_format(json_dict):
    return cp_perf_dict_format(json_dict, contain_max_entry=True)

def cp_FPGA_dict_format(json_dict):
    return cp_perf_dict_format(json_dict, contain_max_entry=True)
  

def get_FPGA_mean_and_error_bar_dict(json_dict):
    """
    Given the key, return a dictionary of std deviation

    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B][max_entry_size]
    perf_set = {"mean": 55.0, "std": 2.12", kernel_mean": 54.1, "kernel_std": 2.0}
    """
    # cp the structure
    dict_mean_error_bar = cp_FPGA_dict_format(json_dict)
    for dataset in json_dict:
        for join_type in json_dict[dataset]:
            for size_dataset_A in json_dict[dataset][join_type]:
                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:
                    for max_entry_size in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                        std = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["time_ms"])
                        mean = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["time_ms"])
                        kernel_std = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["kernel_time_ms"])
                        kernel_mean = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["kernel_time_ms"])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std"] = std
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean"] = mean
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["kernel_std"] = kernel_std
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["kernel_mean"] = kernel_mean
    return dict_mean_error_bar

def get_cpp_mean_and_error_bar_dict(json_dict):
    """
    get mean and std deviation from the cpp profiles
    """
    dict_mean_error_bar = cp_FPGA_dict_format(json_dict)
    for dataset in json_dict:
        for join_type in json_dict[dataset]:
            for size_dataset_A in json_dict[dataset][join_type]:
                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:
                    for max_entry_size in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:

                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_build_index_1_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_1_ms"])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_build_index_2_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_2_ms"])
                        if "sync_traversal_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_sync_traversal_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["sync_traversal_ms"])
                        if "bfs_static_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_bfs_static_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_static_ms"])
                        if "bfs_dfs_static_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_bfs_dfs_static_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dfs_static_ms"])
                        if "bfs_dynamic_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_bfs_dynamic_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dynamic_ms"])
                        if "bfs_dfs_dynamic_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["std_bfs_dfs_dynamic_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dfs_dynamic_ms"])

                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_build_index_1_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_1_ms"])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_build_index_2_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_2_ms"])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_build_both_indexes_ms"] = \
                             np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_1_ms"]) + np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_2_ms"])
                        if "sync_traversal_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_sync_traversal_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["sync_traversal_ms"])
                        if "bfs_static_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_bfs_static_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_static_ms"])
                        if "bfs_dfs_static_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_bfs_dfs_static_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dfs_static_ms"])
                        if "bfs_dynamic_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_bfs_dynamic_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dynamic_ms"])
                        if "bfs_dfs_dynamic_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["mean_bfs_dfs_dynamic_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["bfs_dfs_dynamic_ms"])
    return dict_mean_error_bar
 

def get_cpp_stripe_mean_and_error_bar_dict(json_dict):
    """
    get mean and std deviation from the cpp profiles
    """
    dict_mean_error_bar = cp_FPGA_dict_format(json_dict)
    for dataset in json_dict:
        for join_type in json_dict[dataset]:
            for size_dataset_A in json_dict[dataset][join_type]:
                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:
                    for num_partitions in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:

                        best_join_direction = json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["best_join_direction"]
                        if best_join_direction == 'p0s1':
                            key_partition = 'p0s1_partition_ms'
                        elif best_join_direction == 'p1s0':
                            key_partition = 'p1s0_partition_ms'
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["std_partition_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions][key_partition])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["std_join_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["best_join_ms"])

                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["mean_partition_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions][key_partition])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["mean_join_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]["best_join_ms"])
                
    return dict_mean_error_bar
 

def get_spatialspark_mean_and_error_bar_dict(json_dict):
    """
    Given the key, return a dictionary of std deviation

    perf_set = dict[dataset][join type (Point-in-Polygon or Polygon-Polygon)][size_dataset_A][size_dataset_B][max_entry_size]
    perf_set = {"mean": 55.0, "std": 2.12", kernel_mean": 54.1, "kernel_std": 2.0}
    """
    # cp the structure
    dict_mean_error_bar = cp_FPGA_dict_format(json_dict)
    for dataset in json_dict:
        for join_type in json_dict[dataset]:
            for size_dataset_A in json_dict[dataset][join_type]:
                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:
                    for partition_size in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                        std = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["join_time_ms"])
                        mean = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["join_time_ms"])
                        partition_std = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["partition_time_ms"])
                        partititon_mean = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["partition_time_ms"])
                        index_std = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["build_index_time_ms"])
                        index_mean = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["build_index_time_ms"])
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["std_join_time_ms"] = std
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["mean_join_time_ms"] = mean
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["std_partition_ms"] = partition_std
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["mean_partititon_ms"] = partititon_mean
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["std_index_ms"] = index_std
                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B][partition_size]["mean_index_ms"] = index_mean
    return dict_mean_error_bar
       
def get_software_baseline_error_bar_dict(json_dict):    
    """
    get mean and std deviation from the baseline software profiles
    """
    dict_mean_error_bar = cp_FPGA_dict_format(json_dict)
    for dataset in json_dict:
        for join_type in json_dict[dataset]:
            for size_dataset_A in json_dict[dataset][join_type]:
                for size_dataset_B in json_dict[dataset][join_type][size_dataset_A]:

                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_join_time_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"])
                        if "build_index_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_build_index_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_ms"])
                        if "build_index_1_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_build_index_1_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_1_ms"])
                        if "build_index_2_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_build_index_2_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_2_ms"])
                        if "partition_1_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_partition_1_time_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition_1_time_ms"])
                        if "partition_2_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["std_partition_2_time_ms"] = np.std(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition_2_time_ms"])
                        

                        dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_join_time_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"])
                        if "build_index_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_build_index_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_ms"])
                        if "build_index_1_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_build_index_1_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_1_ms"])
                        if "build_index_2_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_build_index_2_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_2_ms"])
                        if "partition_1_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_partition_1_time_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition_1_time_ms"])
                        if "partition_2_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
                            dict_mean_error_bar[dataset][join_type][size_dataset_A][size_dataset_B]["mean_partition_2_time_ms"] = np.average(json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition_2_time_ms"])
                        
    return dict_mean_error_bar
        
def get_array_from_dict(json_dict, key_sets):
    """
    Given a performance dictionary, and a series of key sets, return the array of values
    example key series:
    [
        ["Uniform", "Point-in-Polygon", "1000000", "1000000", "16", "mean"],
        ["Uniform", "Point-in-Polygon", "1000000", "1000000", "32", "mean"],
        ["Uniform", "Point-in-Polygon", "1000000", "1000000", "64", "mean"],
    ]
    """
    out_array = []
    for key_set in key_sets:
        tmp = json_dict
        for key in key_set:
            tmp = tmp[key]
        out_array.append(tmp)
    return out_array