"""
Run postgis experiments

Example Usage: 
python3 postgis_run_all_experiments.py \
--out_json_fname postgis_runs.json \
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
            first = matches[0]
            if '.' in first:
                first = float(first)
            else:
                first = int(first)
            
            if len(matches) > 2:
                second = matches[2]
                if '.' in second:
                    second = float(second)
                else:
                    second = int(second)
                return (first, second)
            else:
                return (first,)
    return None

parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='postgis_runs.json')
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

print("Postgis benchmarking script")
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
        "build_index_1_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "build_index_2_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "join_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return True
    else:
        return False
    
def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B,
            num_results, build_index_1_ms, build_index_2_ms, join_time_ms):
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
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_1_ms"] = build_index_1_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_2_ms"] = build_index_2_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"] = join_time_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 

os.system('psql -U postgres -f /home/setup.sql')
num_workers = 16
workers = f'set max_parallel_workers = {num_workers}; set max_parallel_workers_per_gather = {num_workers};' 

for dataset in datasets:
    for size_dataset_A in dataset_sizes:
        s = 'uniform' if dataset=='Uniform' else 'OSM'
        pre_path = f'/home/data/postgis_{s}_'
        path_points = f'{pre_path}{size_dataset_A}_point_file_0.csv'
        path_poly_0 = f'{pre_path}{size_dataset_A}_polygon_file_0.csv'
        os.system(f'psql -U postgres -c "copy data_A from \'{path_points}\' delimiter \';\' csv header;"')
        os.system(f'psql -U postgres -c "copy data_C from \'{path_poly_0}\' delimiter \';\' csv header;"')
        for size_dataset_B in dataset_sizes:
            path_poly = f'{pre_path}{size_dataset_B}_polygon_file_0.csv'
            path_poly_1 = f'{pre_path}{size_dataset_B}_polygon_file_1.csv'
            os.system(f'psql -U postgres -c "copy data_B from \'{path_poly}\' delimiter \';\' csv header;"')
            os.system(f'psql -U postgres -c "copy data_D from \'{path_poly_1}\' delimiter \';\' csv header;"')

            array_num_results_pip = [] 
            array_build_index_1_ms_pip = [] 
            array_build_index_2_ms_pip = [] 
            array_join_time_ms_pip = []

            array_num_results_pp = [] 
            array_build_index_1_ms_pp = [] 
            array_build_index_2_ms_pp = [] 
            array_join_time_ms_pp = []

            for _ in range(num_runs):
                #pip
                os.system(f'(psql -U postgres -c "{workers} create index A on data_A using gist(geom);") > log')
                tup = find_numbers('log')
                array_build_index_1_ms_pip.append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} create index B on data_B using gist(geom);") > log')
                tup = find_numbers('log')
                array_build_index_2_ms_pip.append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} select count(*) from data_A join data_B on ST_Intersects(data_A.geom,data_B.geom);") > log')
                tup = find_numbers('log')
                array_num_results_pip.append(tup[0])
                array_join_time_ms_pip.append(tup[1])
                os.system('psql -U postgres -c "drop index A, B;"')

                #pp
                os.system(f'(psql -U postgres -c "{workers} create index C on data_C using gist(geom);") > log')
                tup = find_numbers('log')
                array_build_index_1_ms_pp.append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} create index D on data_D using gist(geom);") > log')
                tup = find_numbers('log')
                array_build_index_2_ms_pp.append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} select count(*) from data_C join data_D on ST_Intersects(data_C.geom,data_D.geom);") > log')
                tup = find_numbers('log')
                array_num_results_pp.append(tup[0])
                array_join_time_ms_pp.append(tup[1])
                os.system('psql -U postgres -c "drop index C, D;"')

            os.system('psql -U postgres -c "delete from data_B;"')
            os.system('psql -U postgres -c "delete from data_D;"')
        
            # pip
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Point-in-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pip, array_build_index_1_ms_pip, array_build_index_2_ms_pip, array_join_time_ms_pip)
            #pp
            write_json(out_json_fname, overwrite, json_dict, 
                        dataset, 'Polygon-Polygon', size_dataset_A, size_dataset_B,
                        array_num_results_pp, array_build_index_1_ms_pp, array_build_index_2_ms_pp, array_join_time_ms_pp)
        
        os.system('psql -U postgres -c "delete from data_A;"')
        os.system('psql -U postgres -c "delete from data_C;"')


with open(f'runs.json', 'w') as file:
    json.dump(json_dict, file, indent = 2)

os.system('psql -U postgres -f /home/clear.sql')
