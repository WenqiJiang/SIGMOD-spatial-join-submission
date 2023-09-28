"""
Run cpp experiments (tree construction, single-thread synchrnous traversal, {static, dynamic} x {bfs, bfs-dfs}),
    using different datasets, data scales, num_partitions , and number of runs

Example Usage: 
python cpp_run_all_stripe_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads_stripes.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/1d_multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--num_runs 3


For testing thread scalability where we only evaluate on a single dataset:
python cpp_run_all_stripe_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_16_threads_stripes.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/1d_multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 16 \
--num_runs 3

"""
import argparse 
import json
import os
import numpy as np
from utils import get_number_file_with_keywords


parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='CPU_perf_16_threads_stripes.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/1d_multithread', help="the CPP exe file")
parser.add_argument('--C_file_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--num_runs', type=int, default=1, help="number of CPU runs")
parser.add_argument('--num_threads', type=int, default=1, help="number of CPU cores & threads in OpenMP, we do not use hyper-threading")

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--join_type', type=str, default=None, help="set a speficic join type (Point-in-Polygon or Polygon-Polygon)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--num_partitions', type=int, default=None, help="set a speficic max R-tree node size")
parser.add_argument('--log_name', type=str, default='cpu_mt_log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
cpp_exe_dir = args.cpp_exe_dir
C_file_dir = args.C_file_dir
num_runs = args.num_runs
num_threads = args.num_threads
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
if args.num_partitions is None:
    num_partitions_list = [100, 1000, 10000, 100000]
else:
    num_partitions_list = [args.num_partitions]

if os.path.exists(out_json_fname):
    with open(out_json_fname, 'r') as f:
        json_dict = json.load(f)
else:
    json_dict = dict()
    
def parse_perf_result(fname):
    """
    Get the performance of a single run from the cpp log
    Return: 
            num_results, partition_ms, join_ms, join_ms
    """
        
    p0s1_num_results = get_number_file_with_keywords(fname, 'Number of results (partition dim 0, sweep dim 1):', "int")
    p0s1_partition_ms = get_number_file_with_keywords(fname, 'Partition the datasets (partition dim 0, sweep dim 1):', "float")
    p0s1_join_ms = get_number_file_with_keywords(fname, 'Join the datasets (partition dim 0, sweep dim 1):', "float")
    p1s0_num_results = get_number_file_with_keywords(fname, 'Number of results (partition dim 1, sweep dim 0):', "int")
    p1s0_partition_ms = get_number_file_with_keywords(fname, 'Partition the datasets (partition dim 1, sweep dim 0):', "float")
    p1s0_join_ms = get_number_file_with_keywords(fname, 'Join the datasets (partition dim 1, sweep dim 0):', "float")
    
    return p0s1_num_results, p0s1_partition_ms, p0s1_join_ms, p1s0_num_results, p1s0_partition_ms, p1s0_join_ms

def config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, num_partitions, 
                         keys=["p0s1_num_results", "p0s1_partition_ms", "p0s1_join_ms",
                               "p1s0_num_results", "p1s0_partition_ms", "p1s0_join_ms",
                               "best_join_direction", "best_join_ms"]):
    """
    Given the input dict and some config, check whether the entry is in the dict

    Note: the key are all in string format in json

    Return: True (already exist) or False (not exist)
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    num_partitions = str(num_partitions)
    if dataset not in json_dict:
        return False
    if join_type not in json_dict[dataset]:
        return False
    if size_dataset_A not in json_dict[dataset][join_type]:
        return False
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        return False
    if num_partitions not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return False
    for key in keys:
        if key not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions]:
            return False
    return True

def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, num_partitions,
            kv = {"p0s1_num_results":[], "p0s1_partition_ms":[], "p0s1_join_ms":[],
                  "p1s0_num_results":[], "p1s0_partition_ms":[], "p1s0_join_ms":[],
                  "best_join_direction":None, "best_join_ms":[]}):
    """
    write the json dictionary, given the input information
    
    Note: the key are all in string format in json
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    num_partitions = str(num_partitions)
    if dataset not in json_dict:
        json_dict[dataset] = dict()
    if join_type not in json_dict[dataset]:
        json_dict[dataset][join_type] = dict()
    if size_dataset_A not in json_dict[dataset][join_type]:
        json_dict[dataset][join_type][size_dataset_A] = dict()
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B] = dict()
    if num_partitions not in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions] = dict()
    
    keys = [key for key in kv]
    if not overwrite and config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B, num_partitions, keys):
        return 
    else:
        for key in kv:
            json_dict[dataset][join_type][size_dataset_A][size_dataset_B][num_partitions][key] = kv[key]

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
                for num_partitions in num_partitions_list:

                    array_p0s1_num_results = [] 
                    array_p0s1_partition_ms = [] 
                    array_p0s1_join_ms = []
                    array_p1s0_num_results = []
                    array_p1s0_partition_ms = []
                    array_p1s0_join_ms = []
                    
                    for reps in range(num_runs):

                        if dataset == 'Uniform':
                            if join_type == "Point-in-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_0_set_0.txt {num_threads} {num_partitions}  > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_uniform_{size_dataset_A}_polygon_file_0_set_0.txt {C_file_dir}/C_uniform_{size_dataset_B}_polygon_file_1_set_0.txt {num_threads} {num_partitions}  > {log_name}'
                        elif dataset == 'OSM':
                            if join_type == "Point-in-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_point_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_0.txt {num_threads} {num_partitions}  > {log_name}'
                            elif join_type == "Polygon-Polygon":
                                cmd = pin_core_prefix + f'{cpp_exe_dir} {C_file_dir}/C_OSM_{size_dataset_A}_polygon_file_0.txt {C_file_dir}/C_OSM_{size_dataset_B}_polygon_file_1.txt {num_threads} {num_partitions}  > {log_name}'
                        
                        print("Executing: ", cmd)
                        os.system(cmd)

                        
                        p0s1_num_results, p0s1_partition_ms, p0s1_join_ms, p1s0_num_results, p1s0_partition_ms, p1s0_join_ms = parse_perf_result(log_name)
                            
                        array_p0s1_num_results.append(p0s1_num_results)
                        array_p0s1_partition_ms.append(p0s1_partition_ms)
                        array_p0s1_join_ms.append(p0s1_join_ms)
                        array_p1s0_num_results.append(p1s0_num_results)
                        array_p1s0_partition_ms.append(p1s0_partition_ms)
                        array_p1s0_join_ms.append(p1s0_join_ms)
                        
                    if np.average(array_p0s1_join_ms) < np.average(array_p1s0_join_ms):
                        best_join_direction = "p0s1"
                        best_join_ms = array_p0s1_join_ms
                    else:
                        best_join_direction = "p1s0"
                        best_join_ms = array_p1s0_join_ms
                        
                    kv = {"p0s1_num_results": array_p0s1_num_results, 
                          "p0s1_partition_ms": array_p0s1_partition_ms, 
                          "p0s1_join_ms": array_p0s1_join_ms,
                          "p1s0_num_results": array_p1s0_num_results,
                          "p1s0_partition_ms": array_p1s0_partition_ms,
                          "p1s0_join_ms": array_p1s0_join_ms,
                          "best_join_direction": best_join_direction,
                          "best_join_ms": best_join_ms}

                    write_json(out_json_fname, overwrite, json_dict, 
                                dataset, join_type, size_dataset_A, size_dataset_B, num_partitions, kv)

print("------------------------------------------------------")
print("CPU experiments complete!")
