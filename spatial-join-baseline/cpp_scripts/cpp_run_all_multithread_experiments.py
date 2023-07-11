"""
Run cpp experiments (tree construction, single-thread synchrnous traversal, {static, dynamic} x {bfs, bfs-dfs}),
    using different datasets, data scales, max_entry node entry sizes, and number of runs

Example Usage: 
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_32_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 32 \
--parallel_approach bfs_dynamic \
--num_runs 1


For testing thread scalability where we only evaluate on a single dataset:
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_16_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 1

"""
import argparse 
import json
import os
from utils import get_number_file_with_keywords


parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='CPU_perf_16_threads.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread', help="the CPP exe file")
parser.add_argument('--C_file_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--num_runs', type=int, default=1, help="number of CPU runs")
parser.add_argument('--num_threads', type=int, default=1, help="number of CPU cores & threads in OpenMP, we do not use hyper-threading")
parser.add_argument('--parallel_approach', type=str, default="bfs_dynamic", help='"all" or "bfs_static" or "bfs_dfs_static" or "bfs_dynamic" or "bfs_dfs_dynamic"')

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--join_type', type=str, default=None, help="set a speficic join type (Point-in-Polygon or Polygon-Polygon)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--max_entry', type=int, default=None, help="set a speficic max R-tree node size")
parser.add_argument('--log_name', type=str, default='cpu_mt_log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
cpp_exe_dir = args.cpp_exe_dir
C_file_dir = args.C_file_dir
num_runs = args.num_runs
num_threads = args.num_threads
parallel_approach = args.parallel_approach
assert parallel_approach in ["all", "bfs_static", "bfs_dfs_static", "bfs_dynamic", "bfs_dfs_dynamic"]
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
    max_entries = [8, 16, 32]
else:
    max_entries = [args.max_entry]

if os.path.exists(out_json_fname):
    with open(out_json_fname, 'r') as f:
        json_dict = json.load(f)
else:
    json_dict = dict()
    
def parse_perf_result(fname, parallel_approach):
    """
    Get the performance of a single run from the cpp log
    Return: 
        if parallel_approach == "all":
            num_results, build_index_1_ms, build_index_2_ms, bfs_static_ms, bfs_dfs_static_ms, bfs_dynamic_ms, bfs_dfs_dynamic_ms
        else:
            num_results, build_index_1_ms, build_index_2_ms, join_ms
    """

    num_results = get_number_file_with_keywords(fname, 'Number of results', "int")
    build_index_1_ms = get_number_file_with_keywords(fname, 'Building RTree for trace 1:', "float")
    build_index_2_ms = get_number_file_with_keywords(fname, 'Building RTree for trace 2:', "float")
    if parallel_approach == "bfs_static": 
        bfs_static_ms = get_number_file_with_keywords(fname, 'BFS + static duration:', "float")
        return num_results, build_index_1_ms, build_index_2_ms, bfs_static_ms
    elif parallel_approach == "bfs_dfs_static": 
        bfs_dfs_static_ms = get_number_file_with_keywords(fname, 'BFS-DFS + static duration:', "float")
        return num_results, build_index_1_ms, build_index_2_ms, bfs_dfs_static_ms
    elif parallel_approach == "bfs_dynamic":
        bfs_dynamic_ms = get_number_file_with_keywords(fname, 'BFS + dynamic duration:', "float")
        return num_results, build_index_1_ms, build_index_2_ms, bfs_dynamic_ms
    elif parallel_approach == "bfs_dfs_dynamic":
        bfs_dfs_dynamic_ms = get_number_file_with_keywords(fname, 'BFS-DFS + dynamic duration:', "float")
        return num_results, build_index_1_ms, build_index_2_ms, bfs_dfs_dynamic_ms
    elif parallel_approach == "all":
        bfs_static_ms = get_number_file_with_keywords(fname, 'BFS + static duration:', "float")
        bfs_dfs_static_ms = get_number_file_with_keywords(fname, 'BFS-DFS + static duration:', "float")
        bfs_dynamic_ms = get_number_file_with_keywords(fname, 'BFS + dynamic duration:', "float")
        bfs_dfs_dynamic_ms = get_number_file_with_keywords(fname, 'BFS-DFS + dynamic duration:', "float")
        
        return num_results, build_index_1_ms, build_index_2_ms, \
            bfs_static_ms, bfs_dfs_static_ms, bfs_dynamic_ms, bfs_dfs_dynamic_ms

def config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size, 
                         keys=["num_results", "build_index_1_ms", "build_index_2_ms", "bfs_static_ms", "bfs_dfs_static_ms", "bfs_dynamic_ms", "bfs_dfs_dynamic_ms"]):
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
    for key in keys:
        if key not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
            return False
    return True

def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size,
            kv = {"num_results":[], "build_index_1_ms":[], "build_index_2_ms":[], "bfs_static_ms":[], "bfs_dfs_static_ms":[], "bfs_dynamic_ms":[], "bfs_dfs_dynamic_ms":[]}):
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
    
    keys = [key for key in kv]
    if not overwrite and config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size, keys):
        return 
    else:
        for key in kv:
            json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size][key] = kv[key]

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 

if num_threads == 1:
    pin_core_prefix = 'taskset --cpu-list {} '.format(0)
else:
    pin_core_prefix = 'taskset --cpu-list {}-{} '.format(0, num_threads - 1)

for dataset in datasets:
    for join_type in join_types:
        for size_dataset_A in dataset_sizes:
            for size_dataset_B in dataset_sizes:
                for max_entry_size in max_entries:

                    array_num_results = [] 
                    array_build_index_1_ms = [] 
                    array_build_index_2_ms = [] 
                    array_bfs_static_ms = [] 
                    array_bfs_dfs_static_ms = [] 
                    array_bfs_dynamic_ms = [] 
                    array_bfs_dfs_dynamic_ms = []
                    
                    for reps in range(num_runs):

                        if dataset == 'Uniform':
                            if join_type == "Point-in-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_0_set_0.txt {parallel_approach} {max_entry_size} {num_threads} > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_polygon_file_0_set_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_1_set_0.txt {parallel_approach} {max_entry_size} {num_threads} > {log_name}'
                        elif dataset == 'OSM':
                            if join_type == "Point-in-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_0.txt {parallel_approach} {max_entry_size} {num_threads} > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_polygon_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_1.txt {parallel_approach} {max_entry_size} {num_threads} > {log_name}'
                        
                        print("Executing: ", cmd)
                        os.system(cmd)

                        if parallel_approach == "all":
                            num_results, build_index_1_ms, build_index_2_ms, \
                                bfs_static_ms, bfs_dfs_static_ms, bfs_dynamic_ms, bfs_dfs_dynamic_ms = parse_perf_result(log_name, parallel_approach)

                            array_bfs_static_ms.append(bfs_static_ms)
                            array_bfs_dfs_static_ms.append(bfs_dfs_static_ms)
                            array_bfs_dynamic_ms.append(bfs_dynamic_ms)
                            array_bfs_dfs_dynamic_ms.append(bfs_dfs_dynamic_ms)
                        else:
                            num_results, build_index_1_ms, build_index_2_ms, join_ms = \
                                parse_perf_result(log_name, parallel_approach)
                            if parallel_approach == "bfs_static":
                                array_bfs_static_ms.append(join_ms)
                            if parallel_approach == "bfs_dfs_static":
                                array_bfs_dfs_static_ms.append(join_ms)
                            if parallel_approach == "bfs_dynamic":
                                array_bfs_dynamic_ms.append(join_ms)
                            if parallel_approach == "bfs_dfs_dynamic":
                                array_bfs_dfs_dynamic_ms.append(join_ms)
                            
                        array_num_results.append(num_results)
                        array_build_index_1_ms.append(build_index_1_ms)
                        array_build_index_2_ms.append(build_index_2_ms)
                        
                    kv = {"num_results": array_num_results, 
                          "build_index_1_ms": array_build_index_1_ms, 
                          "build_index_2_ms": array_build_index_2_ms}
                    if parallel_approach == "all" or parallel_approach == "bfs_static":
                        kv["bfs_static_ms"] = array_bfs_static_ms
                    if parallel_approach == "all" or parallel_approach == "bfs_dfs_static":
                        kv["bfs_dfs_static_ms_ms"] = array_bfs_dfs_static_ms
                    if parallel_approach == "all" or parallel_approach == "bfs_dynamic":
                        kv["bfs_dynamic_ms"] = array_bfs_dynamic_ms
                    if parallel_approach == "all" or parallel_approach == "bfs_dfs_dynamic":
                        kv["bfs_dfs_dynamic_ms"] = array_bfs_dfs_dynamic_ms

                    write_json(out_json_fname, overwrite, json_dict, 
                                dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size, kv)

print("------------------------------------------------------")
print("CPU experiments complete!")
