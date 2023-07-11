"""
Run sedona experiments

Example Usage: 
python sedona_run_all_experiments.py \
--out_json_fname sedona_runs.json \
--overwrite 0 \
--num_runs 3
"""

import os
import json
import re
import argparse 

def find_numbers(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        matches = re.findall(r'\b\d+(?:\.\d+)?(?:0+)?\b', content)
        if matches:
            try:
                part1 = float(matches[0])*1000
                part2 = float(matches[1])*1000
                build = float(matches[2])*1000
                num = int(matches[3])
                join = float(matches[4])*1000
                return(part1, part2, build, num, join)
            except:
                return None
            
    return None

parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='sedona_runs.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--num_runs', type=int, default=1, help="number of runs")

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--log_name', type=str, default='log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
num_runs = args.num_runs
log_name = args.log_name

print("Sedona benchmarking script")
print("------------------------------------------------------")

if args.dataset is None:
    datasets = ['Uniform','OSM']
else:
    assert args.dataset in ['Uniform','OSM']
    datasets = [args.dataset]
join_types = ["Point-in-Polygon", "Polygon-Polygon"]
if args.dataset_size is None:
    dataset_sizes = [int(1e5), int(1e6), int(1e7)]
else:
    dataset_sizes = [args.dataset_size]

if os.path.exists(out_json_fname):
    with open(out_json_fname, 'r') as f:
        json_dict = json.load(f)
else:
    json_dict = dict()

def config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B):
    """
    Given the input dict and some config, check whether the entry is in the dict

    Note: the key are all in string format in json

    Return: True (already exist) or False (not exist)
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    if dataset not in json_dict:
        return False
    if join_type not in json_dict[dataset]:
        return False
    if size_dataset_A not in json_dict[dataset][join_type]:
        return False
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        return False
    if "num_results" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "partition1_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "partition2_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "build_index_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "join_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return True
    else:
        return False
    
def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B,
            num_results, partition1_time_ms, partition2_time_ms, build_index_time_ms, join_time_ms):
    """
    write the json dictionary, given the input information
    
    Note: the key are all in string format in json
    """
    size_dataset_A = str(size_dataset_A)
    size_dataset_B = str(size_dataset_B)
    if dataset not in json_dict:
        json_dict[dataset] = dict()
    if join_type not in json_dict[dataset]:
        json_dict[dataset][join_type] = dict()
    if size_dataset_A not in json_dict[dataset][join_type]:
        json_dict[dataset][join_type][size_dataset_A] = dict()
    if size_dataset_B not in json_dict[dataset][join_type][size_dataset_A]:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B] = dict()
    
    if not overwrite and config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B):
        return 
    else:
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["num_results"] = num_results
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition1_time_ms"] = partition1_time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition2_time_ms"] = partition2_time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_time_ms"] = build_index_time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"] = join_time_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 

for dataset in datasets:
    for size_dataset_A in dataset_sizes:
        for size_dataset_B in dataset_sizes:

            array_num_results_pip = [] 
            array_partition1_time_ms_pip = [] 
            array_partition2_time_ms_pip = [] 
            array_build_index_time_ms_pip = []
            array_join_time_ms_pip = []
            
            array_num_results_pp = [] 
            array_partition1_time_ms_pp = [] 
            array_partition2_time_ms_pp = [] 
            array_build_index_time_ms_pp = []
            array_join_time_ms_pp = []

            for _ in range(num_runs):

                if dataset == "Uniform":
                    #pip
                    file_A = f'postgis_uniform_{size_dataset_A}_point_file_0'
                    file_B = f'postgis_uniform_{size_dataset_B}_polygon_file_0_set_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    array_num_results_pip.append(tup[3])
                    array_partition1_time_ms_pip.append(tup[0])
                    array_partition2_time_ms_pip.append(tup[1])
                    array_build_index_time_ms_pip.append(tup[2])
                    array_join_time_ms_pip.append(tup[4])
                    #pp
                    file_A = f'postgis_uniform_{size_dataset_A}_polygon_file_0_set_0'
                    file_B = f'postgis_uniform_{size_dataset_B}_polygon_file_1_set_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    array_num_results_pp.append(tup[3])
                    array_partition1_time_ms_pp.append(tup[0])
                    array_partition2_time_ms_pp.append(tup[1])
                    array_build_index_time_ms_pp.append(tup[2])
                    array_join_time_ms_pp.append(tup[4])
                elif dataset == 'OSM':
                    #pip
                    file_A = f'postgis_OSM_{size_dataset_A}_point_file_0'
                    file_B = f'postgis_OSM_{size_dataset_B}_polygon_file_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    array_num_results_pip.append(tup[3])
                    array_partition1_time_ms_pip.append(tup[0])
                    array_partition2_time_ms_pip.append(tup[1])
                    array_build_index_time_ms_pip.append(tup[2])
                    array_join_time_ms_pip.append(tup[4])
                    #pp
                    file_A = f'postgis_OSM_{size_dataset_A}_polygon_file_0'
                    file_B = f'postgis_OSM_{size_dataset_B}_polygon_file_1'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    array_num_results_pp.append(tup[3])
                    array_partition1_time_ms_pp.append(tup[0])
                    array_partition2_time_ms_pp.append(tup[1])
                    array_build_index_time_ms_pp.append(tup[2])
                    array_join_time_ms_pp.append(tup[4])
        
            # pip
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Point-in-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pip, array_partition1_time_ms_pip, array_partition2_time_ms_pip, array_build_index_time_ms_pip, array_join_time_ms_pip)
            #pp
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Polygon-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pp, array_partition1_time_ms_pp, array_partition2_time_ms_pp, array_build_index_time_ms_pp, array_join_time_ms_pp)

print("------------------------------------------------------")
print("Sedona experiments complete!")