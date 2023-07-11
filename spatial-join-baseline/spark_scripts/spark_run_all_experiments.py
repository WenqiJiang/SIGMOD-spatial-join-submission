"""
Run spark experiments

Example Usage: 
python3 spark_run_all_experiments.py \
--out_json_fname spark_runs.json \
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
                part = int(matches[0])
                build = int(matches[2])
                num = int(matches[4])
                join = int(matches[5])
                return(part, build, num, join)
            except:
                return None
            
    return None

parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='spark_runs.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--num_runs', type=int, default=1, help="number of runs")
parser.add_argument('--partitions', type=int, default=64, help="number of data partitions")

parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--log_name', type=str, default='log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
num_runs = args.num_runs
partitions = args.partitions
log_name = args.log_name

print("SpatialSpark benchmarking script")
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
        "partition_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "build_index_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "join_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return True
    else:
        return False
    
def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B,
            num_results, partition_time_ms, build_index_time_ms, join_time_ms):
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
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["partition_time_ms"] = partition_time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_time_ms"] = build_index_time_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"] = join_time_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return

for dataset in datasets:
    s = "uniform" if dataset == "Uniform" else "OSM"
    for size_dataset_A in dataset_sizes:
        #pip
        file_A_pip = f'/home/SpatialSpark/data/spatialspark_{s}_{size_dataset_A}_point_file_0.txt'
        #pp
        file_A_pp = f'/home/SpatialSpark/data/spatialspark_{s}_{size_dataset_A}_polygon_file_0.txt'
        for size_dataset_B in dataset_sizes:
            #pip
            file_B_pip = f'/home/SpatialSpark/data/spatialspark_{s}_{size_dataset_B}_polygon_file_0.txt'
            #pp
            file_B_pp = f'/home/SpatialSpark/data/spatialspark_{s}_{size_dataset_B}_polygon_file_1.txt'

            array_num_results_pip = [] 
            array_partition_time_ms_pip = [] 
            array_build_index_time_ms_pip = []
            array_join_time_ms_pip = []
            
            array_num_results_pp = [] 
            array_partition_time_ms_pp = [] 
            array_build_index_time_ms_pp = []
            array_join_time_ms_pp = []

            #pip
            cmd = f'(/usr/bin/time /opt/spark-2.0.2/bin/spark-submit --driver-memory 150g --executor-memory 70g --conf spark.driver.maxResultSize=4g --conf spark.kryoserializer.buffer.max=2047m --master spark://d9336c0d81a5:7077 --class spatialspark.main.SpatialJoinApp /home/timing/target/scala-2.11/spatial-spark-assembly-1.1.1-beta-SNAPSHOT.jar --left {file_A_pip} --geom_left 0 --right {file_B_pip} --geom_right 0 --broadcast false --output {partitions} --predicate intersects --partition {partitions} --method stp --conf 32:32:0.1 --num_output 1) > log'
            for _ in range(num_runs):
                try:
                    os.system(cmd)
                    os.system(f'rm -r {partitions}')
                    tup = find_numbers('log')
                    
                    array_num_results_pip.append(tup[2])
                    array_partition_time_ms_pip.append(tup[0])
                    array_build_index_time_ms_pip.append(tup[1])
                    array_join_time_ms_pip.append(tup[3])
                except:
                    continue

            #pp
            cmd = f'(/usr/bin/time /opt/spark-2.0.2/bin/spark-submit --driver-memory 150g --executor-memory 70g --conf spark.driver.maxResultSize=4g --conf spark.kryoserializer.buffer.max=2047m --master spark://d9336c0d81a5:7077 --class spatialspark.main.SpatialJoinApp /home/timing/target/scala-2.11/spatial-spark-assembly-1.1.1-beta-SNAPSHOT.jar --left {file_A_pp} --geom_left 0 --right {file_B_pp} --geom_right 0 --broadcast false --output {partitions} --predicate intersects --partition {partitions} --method stp --conf 32:32:0.1 --num_output 1) > log'
            for _ in range(num_runs):
                try:
                    os.system(cmd)
                    os.system(f'rm -r {partitions}')
                    tup = find_numbers('log')
                    array_num_results_pp.append(tup[2])
                    array_partition_time_ms_pp.append(tup[0])
                    array_build_index_time_ms_pp.append(tup[1])
                    array_join_time_ms_pp.append(tup[3])
                except:
                    continue

            # pip
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Point-in-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pip, array_partition_time_ms_pip, array_build_index_time_ms_pip, array_join_time_ms_pip)
            #pp
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Polygon-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pp, array_partition_time_ms_pp, array_build_index_time_ms_pp, array_join_time_ms_pp)


with open(f'results.json', 'w') as file:
    json.dump(json_dict, file, indent = 2)
