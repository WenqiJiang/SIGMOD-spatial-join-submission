"""
Run cuspatial experiments (data movement between CPU and GPU, tree construction, 
    using different datasets, data scales, and number of runs

Example Usage: 
python cuspatial_run_all_experiments.py \
--out_json_fname GPU_perf_3_runs.json \
--overwrite 0 \
--python_dir /mnt/scratch/wenqi/spatial-join-baseline/cuspatial_scripts/pip_join.py \
--data_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_runs 3 \
--avg_leaf_size 16 \
--batch_size 50000
"""
import argparse 
import json
import os
import re


def get_number_file_with_keywords(fname, keyword, dtype='int'):
	"""
	Given an input text file, find the line that contains a keyword,
		and extract the first number (int or float)
	"""
	assert dtype == 'int' or dtype == 'float'
	if dtype == 'int':
		pattern = r"\d+"
	elif dtype == 'float':
		pattern = r"(\d+.\d+)"

	result = None
	with open(fname) as f:
		lines = f.readlines()
		for line in lines:
			if keyword in line:
				result = re.findall(pattern, line)[0]
	assert result is not None
	if dtype == 'int':
		return int(result)
	elif dtype == 'float':
		return float(result)


parser = argparse.ArgumentParser()
parser.add_argument('--out_json_fname', type=str, default='GPU_perf.json')
parser.add_argument('--overwrite', type=int, default=0, help="0: skip existing results, 1: overwrite")
parser.add_argument('--python_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cuspatial_scripts/pip_join.py', help="the CPP exe file")
parser.add_argument('--data_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--num_runs', type=int, default=1, help="number of CPU runs")
parser.add_argument('--dataset', type=str, default=None, help="set a speficic data name (OSM or Uniform)")
parser.add_argument('--dataset_size', type=int, default=None, help="set a speficic data size")
parser.add_argument('--avg_leaf_size', type=int, default=16, help="Maxmimum average leaf size in quad-tree")
parser.add_argument('--batch_size', type=int, default=10000, help="GPU batch size (polygon as window queries)")
parser.add_argument('--log_name', type=str, default='gpu_log', help="output log")

args = parser.parse_args()
out_json_fname = args.out_json_fname
overwrite = args.overwrite
python_dir = args.python_dir
data_dir = args.data_dir
num_runs = args.num_runs
avg_leaf_size = args.avg_leaf_size
batch_size = args.batch_size
log_name = args.log_name
join_types = ["Point-in-Polygon"]

print("CPU benchmarking script")
print("------------------------------------------------------")

if args.dataset is None:
    datasets = ['Uniform','OSM']
else:
    assert args.dataset in ['Uniform','OSM']
    datasets = [args.dataset]
if args.dataset_size is None:
    dataset_sizes = [int(1e5), int(1e6), int(1e7)]
else:
    dataset_sizes = [args.dataset_size]

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
        
    num_results = get_number_file_with_keywords(fname, 'total join pairs:', "int")
    cpu_to_gpu_ms = get_number_file_with_keywords(fname, 'time data structure convert and data movement CPU -> GPU:', "float")
    build_index_ms = get_number_file_with_keywords(fname, 'time index:', "float")
    join_time_ms = get_number_file_with_keywords(fname, 'time join:', "float")

    return num_results, cpu_to_gpu_ms, build_index_ms, join_time_ms

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
        "cpu_to_gpu_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "build_index_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B] and \
        "join_time_ms" in json_dict[dataset][join_type][size_dataset_A][size_dataset_B]:
        return True
    else:
        return False

def write_json(fname, overwrite, json_dict, 
            dataset, join_type, size_dataset_A, size_dataset_B, 
            num_results, cpu_to_gpu_ms, build_index_ms, join_time_ms):
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
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["cpu_to_gpu_time_ms"] = cpu_to_gpu_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["build_index_time_ms"] = build_index_ms
        json_dict[dataset][join_type][size_dataset_A][size_dataset_B]["join_time_ms"] = join_time_ms

    with open(fname, 'w') as file:
        json.dump(json_dict, file, indent=2)
    return 


for dataset in datasets:
    for join_type in join_types:
        for size_dataset_A in dataset_sizes:
            for size_dataset_B in dataset_sizes:
                    if config_exist_in_dict(json_dict, dataset, join_type, size_dataset_A, size_dataset_B):
                        print("Content exists, skip...", dataset, join_type, size_dataset_A, size_dataset_B) 
                        continue

                    array_num_results = [] 
                    array_cpu_to_gpu_ms = [] 
                    array_build_index_ms = [] 
                    array_join_ms = []

                    for reps in range(num_runs):

                        if dataset == 'Uniform':
                                cmd = f'python {python_dir} --point_file_dir {data_dir}/cuspatial_uniform_{size_dataset_A}_point_file_0.csv --polygon_file_dir {data_dir}/cuspatial_uniform_{size_dataset_B}_polygon_file_0_set_0.csv --avg_leaf_size {avg_leaf_size} --batch_size {batch_size} > {log_name}'
                        elif dataset == 'OSM':
                                cmd = f'python {python_dir} --point_file_dir {data_dir}/cuspatial_OSM_{size_dataset_A}_point_file_0.csv --polygon_file_dir {data_dir}/cuspatial_OSM_{size_dataset_B}_polygon_file_0.csv --avg_leaf_size {avg_leaf_size} --batch_size {batch_size} > {log_name}'

                        print("Executing: ", cmd)
                        os.system(cmd)

                        num_results, cpu_to_gpu_ms, build_index_ms, join_time_ms = parse_perf_result(log_name)
                        array_num_results.append(num_results)
                        array_cpu_to_gpu_ms.append(cpu_to_gpu_ms)
                        array_build_index_ms.append(build_index_ms)
                        array_join_ms.append(join_time_ms)

                    write_json(out_json_fname, overwrite, json_dict, 
                                dataset, join_type, size_dataset_A, size_dataset_B, 
                                array_num_results, array_cpu_to_gpu_ms, array_build_index_ms, array_join_ms)

print("------------------------------------------------------")
print("CPU experiments complete!")
