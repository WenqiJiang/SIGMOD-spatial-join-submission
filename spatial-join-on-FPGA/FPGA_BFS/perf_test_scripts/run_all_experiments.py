"""
This script runs perf_test.py to execute a number of experiments

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

Example Usage:
python run_all_experiments.py \
--out_json_fname FPGA_perf.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_runs 3 > log_run_all_experiments

"""
import os
import numpy as np
import argparse 
import json

from utils import assert_keywords_in_file, get_number_file_with_keywords

parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='FPGA_perf.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
# parser.add_argument('--datasets', type=str, default='both', help='both or Uniform or OSM')
# parser.add_argument('--join_types', type=str, default='host', help="the name of the exe of the FPGA host")
parser.add_argument('--FPGA_project_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE')
parser.add_argument('--FPGA_host_name', type=str, default='host', help="the name of the exe of the FPGA host")
parser.add_argument('--FPGA_bin_name', type=str, default='xclbin/vadd.hw.xclbin', help="the name (as well as the subdir) of the FPGA bitstream")
parser.add_argument('--FPGA_log_name', type=str, default='summary.csv', help="the name of the FPGA perf summary file")
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor', help="the CPP exe file")
parser.add_argument('--C_file_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--get_tree_depth_py_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py', help="the get tree depth file dir")
parser.add_argument('--num_runs', type=int, default=1, help="number of FPGA runs")

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--join_type', type=str, default=None, help="set a speficic join type (Point-in-Polygon or Polygon-Polygon)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--max_entry', type=int, default=None, help="set a speficic max R-tree node size")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite

FPGA_project_dir = args.FPGA_project_dir
FPGA_host_name = args.FPGA_host_name
FPGA_bin_name = args.FPGA_bin_name
FPGA_log_name = args.FPGA_log_name
cpp_exe_dir = args.cpp_exe_dir
C_file_dir = args.C_file_dir
get_tree_depth_py_dir = args.get_tree_depth_py_dir
num_runs = args.num_runs

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

def parse_perf_result(fname, num_runs):
    """
    Get the performance of multiple runs from the perf test script log
    Return: e2e time array, and kernel time array
    """

    time_ms = []
    kernel_time_ms = []
    for run_id in range(num_runs):
        # Run {} FPGA end-to-end: {} ms
        # Run {} Run {} FPGA kernel: {} ms
        e2e_keyword = f"Run {run_id} FPGA end-to-end:"
        kernel_keyword = f"Run {run_id} FPGA kernel:"
        time_ms.append(get_number_file_with_keywords(fname, e2e_keyword, "float"))
        kernel_time_ms.append(get_number_file_with_keywords(fname, kernel_keyword, "float"))
    return time_ms, kernel_time_ms

def config_exist_in_dict(json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size):
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
        "time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "kernel_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
        return True
    else:
        return False

def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size,
            num_results, time_ms, kernel_time_ms):
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
    
    if not overwrite and \
        "num_results" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size] and \
        "kernel_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]:
        return 
    else:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["num_results"] = num_results
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["time_ms"] = time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B][max_entry_size]["kernel_time_ms"] = kernel_time_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 

if os.path.exists(out_json_fname):
    with open(out_json_fname, 'r') as f:
        json_dict = json.load(f)
else:
    json_dict = dict()

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

                C_file_A = os.path.join(C_file_dir, file_A)
                C_file_B = os.path.join(C_file_dir, file_B)

                """ Now the FPGA has bugs when the first tree level > second tree"""
                def swap(name1, name2):
                    return name2, name1
                
                if size_dataset_A > size_dataset_B:
                    C_file_A, C_file_B = swap(C_file_A, C_file_B)

                for max_entry_size in max_entries:
                    
                    print("", flush=True)
                    print(f"Config: dataset:{dataset}, join_type:{join_type}, size_dataset_A:{size_dataset_A}, size_dataset_B:{size_dataset_B}, max_entry_size:{max_entry_size}")
                    # check whether to run
                    if not overwrite and \
                        config_exist_in_dict(json_dict, 
                            dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size):
                        print("Skip, already in dict")
                        continue
                    
                    log_perf_test = 'log_perf_test'
                    cmd_perf_test = "python perf_test.py " + \
                        f" --FPGA_project_dir {FPGA_project_dir} " + \
                        f" --FPGA_host_name {FPGA_host_name} " + \
                        f" --FPGA_bin_name {FPGA_bin_name} " + \
                        f" --FPGA_log_name {FPGA_log_name} " + \
                        f" --cpp_exe_dir {cpp_exe_dir} " + \
                        f" --C_file_A {C_file_A}" + \
                        f" --C_file_B {C_file_B}" + \
                        f" --get_tree_depth_py_dir {get_tree_depth_py_dir} " + \
                        f" --max_entry_size {max_entry_size} " + \
                        f" --num_runs {num_runs}" + \
                        f"  > {log_perf_test}"
                    print("Running perf test script:\n{}".format(cmd_perf_test)) 
                    os.system(cmd_perf_test)

                    # parse log, assert result correct
                    time_ms, kernel_time_ms = parse_perf_result(log_perf_test, num_runs)
                    num_results = get_number_file_with_keywords(log_perf_test, "Number of results:", "int")

                    write_json(out_json_fname, overwrite, json_dict, 
                        dataset, join_type, size_dataset_A, size_dataset_B, max_entry_size,
                        num_results, time_ms, kernel_time_ms)
                    
