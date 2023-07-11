"""
Run cpp experiments (tree construction, single-thread synchrnous traversal, {static, dynamic} x {bfs, bfs-dfs}),
    using different datasets, data scales, max_entry node entry sizes, and number of runs

Example Usage: 
python cpp_run_all_singlethread_experiments.py \
--out_json_fname CPU_perf_singlethread_sync_traversal.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/singlethread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--max_entry 16 \
--num_runs 3

"""
import argparse 
import json
import os
from utils import get_number_file_with_keywords


parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='CPU_perf_16_threads.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/singlethread', help="the CPP exe file")
parser.add_argument('--C_file_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--num_runs', type=int, default=1, help="number of CPU runs")

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--join_type', type=str, default=None, help="set a speficic join type (Point-in-Polygon or Polygon-Polygon)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--max_entry', type=int, default=None, help="set a speficic max R-tree node size")
parser.add_argument('--log_name', type=str, default='cpu_st_log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
cpp_exe_dir = args.cpp_exe_dir
C_file_dir = args.C_file_dir
num_runs = args.num_runs
log_name = args.log_name

print("CPU benchmarking script")
print("------------------------------------------------------")

if args.dataset is None:
    datasets = ['Uniform','OSM']
else:
    assert args.dataset in ['Uniform','OSM']
    datasets = [args.dataset]
if args.join_type is None:
    join_types = ["Point-in-Polygon", "Polygon-Polygon"]
else:
    args.join_type in ["Point-in-Polygon", "Polygon-Polygon"]
    join_types = [args.join_type]
if args.dataset_size is None:
    dataset_sizes = [int(1e5), int(1e6), int(1e7)]
else:
    dataset_sizes = [args.dataset_size]
if args.max_entry is None:
    max_entries = [16] # larger sizes are very slow for single-thread sync-traversal
else:
    max_entries = [args.max_entry]

if os.path.exists(out_json_fname):
    with open(out_json_fname, 'r') as f:
        json_dict = json.load(f)
else:
    json_dict = dict()
    
def parse_perf_result(fname):
    """
    Get the performance of a single run from the cpp log
    Return: num_results, build_index_1_ms, build_index_2_ms, sync_traversal_ms, bfs_static_ms, bfs_dfs_static_ms, bfs_dynamic_ms, bfs_dfs_dynamic_ms
    """
        
    num_results = get_number_file_with_keywords(fname, 'Number of results:', "int")
    build_index_1_ms = get_number_file_with_keywords(fname, 'Building RTree for trace 1:', "float")
    build_index_2_ms = get_number_file_with_keywords(fname, 'Building RTree for trace 2:', "float")
    sync_traversal_ms = get_number_file_with_keywords(fname, 'Sync traversal duration:', "float")

    return num_results, build_index_1_ms, build_index_2_ms, sync_traversal_ms

def config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size):
    """
    Given the input dict and some config, check whether the entry is in the dict

    Note: the key are all in string format in json

    Return: True (already exist) or False (not exist)
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    max_entry_size = str(max_entry_size)
    if dataset not in json_dict:
        return False
    if join_type not in json_dict[dataset]:
        return False
    if size_dataset_A not in json_dict[dataset][join_type]:
        return False
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        return False
    if max_entry_size not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return False
    if "num_results" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "build_index_1_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "build_index_2_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "sync_traversal_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
        return True
    else:
        return False

def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size,
            num_results, build_index_1_ms, build_index_2_ms, sync_traversal_ms):
    """
    write the json dictionary, given the input information
    
    Note: the key are all in string format in json
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    max_entry_size = str(max_entry_size)
    if dataset not in json_dict:
        json_dict[dataset] = dict()
    if join_type not in json_dict[dataset]:
        json_dict[dataset][join_type] = dict()
    if size_dataset_A not in json_dict[dataset][join_type]:
        json_dict[dataset][join_type][size_dataset_A] = dict()
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B] = dict()
    if max_entry_size not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] = dict()
    
    if not overwrite and config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size):
        return 
    else:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["num_results"] = num_results
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_1_ms"] = build_index_1_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["build_index_2_ms"] = build_index_2_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["sync_traversal_ms"] = sync_traversal_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 


for dataset in datasets:
    for join_type in join_types:
        for size_dataset_A in dataset_sizes:
            for size_dataset_B in dataset_sizes:
                for max_entry_size in max_entries:

                    array_num_results = [] 
                    array_build_index_1_ms = [] 
                    array_build_index_2_ms = [] 
                    array_sync_traversal_ms = []
                    array_bfs_static_ms = [] 
                    array_bfs_dfs_static_ms = [] 
                    array_bfs_dynamic_ms = [] 
                    array_bfs_dfs_dynamic_ms = []
                    
                    for reps in range(num_runs):

                        if dataset == 'Uniform':
                            if join_type == "Point-in-Polygon":
                                cmd = f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_0_set_0.txt {max_entry_size} > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_polygon_file_0_set_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_1_set_0.txt {max_entry_size} > {log_name}'
                        elif dataset == 'OSM':
                            if join_type == "Point-in-Polygon":
                                cmd = f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_0.txt {max_entry_size} > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_polygon_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_1.txt {max_entry_size} > {log_name}'
                        
                        print("Executing: ", cmd)
                        os.system(cmd)

                        num_results, build_index_1_ms, build_index_2_ms, sync_traversal_ms = parse_perf_result(log_name)
                        array_num_results.append(num_results)
                        array_build_index_1_ms.append(build_index_1_ms)
                        array_build_index_2_ms.append(build_index_2_ms)
                        array_sync_traversal_ms.append(sync_traversal_ms)

                    write_json(out_json_fname, overwrite, json_dict, 
                                dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size,
                                array_num_results, array_build_index_1_ms, array_build_index_2_ms, array_sync_traversal_ms)

print("------------------------------------------------------")
print("CPU experiments complete!")
