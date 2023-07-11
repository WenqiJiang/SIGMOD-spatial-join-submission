"""
Test the join performance on a single case. 

This script invoke the C++ sync traversal program first to get the built trees 
	from the selected datasets. It then calculate the tree depth, and pass the 
	parameters to the FPGA program. Finally, it gets the FPGA performance in both
	e2e and kernel execution.

Example Usage:

python perf_test.py \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_A /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_0_set_0.txt \
--C_file_B /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_1_set_0.txt \
--max_entry_size 16 --num_runs 3
"""

import os
import re
import numpy as np
import argparse 

from utils import assert_keywords_in_file, get_number_file_with_keywords

parser = argparse.ArgumentParser()
parser.add_argument('--FPGA_project_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE')
parser.add_argument('--FPGA_host_name', type=str, default='host', help="the name of the exe of the FPGA host")
parser.add_argument('--FPGA_bin_name', type=str, default='xclbin/vadd.hw.xclbin', help="the name (as well as the subdir) of the FPGA bitstream")
parser.add_argument('--FPGA_log_name', type=str, default='summary.csv', help="the name of the FPGA perf summary file")
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor', help="the CPP exe file")
parser.add_argument('--C_file_A', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_0_set_0.txt', help="the CPP input file")
parser.add_argument('--C_file_B', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_1_set_0.txt', help="the CPP input file")
parser.add_argument('--get_tree_depth_py_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py', help="the get tree depth file dir")
parser.add_argument('--max_entry_size', type=int, default=32, help="the max entry numbers in an R tree node")
parser.add_argument('--num_runs', type=int, default=1, help="number of FPGA runs")

args = parser.parse_args()
FPGA_project_dir = args.FPGA_project_dir
FPGA_host_name = args.FPGA_host_name
FPGA_bin_name = args.FPGA_bin_name
FPGA_log_name = args.FPGA_log_name
cpp_exe_dir = args.cpp_exe_dir
C_file_A = args.C_file_A
C_file_B = args.C_file_B
max_entry_size = args.max_entry_size
get_tree_depth_py_dir = args.get_tree_depth_py_dir
num_runs = args.num_runs


def get_tree_depth_procedure(tree="A", node_bytes=4096):
	"""
	Input: "A" or "B"
	Output: depth
	"""
	assert tree == 'A' or tree == 'B'

	log_tree_depth = f'log_tree_depth_{tree}'
	cmd_depth = "python {} --tree_bin_dir {} --node_bytes {} > {}".format(
		get_tree_depth_py_dir, f'tree_{tree}.bin', node_bytes, log_tree_depth)
	print("Get tree depth {} command:\n{}".format(tree, cmd_depth))
	os.system(cmd_depth)
	level = get_number_file_with_keywords(log_tree_depth, "loaded index max depth:", "int")
	
	return level

def get_FPGA_summary_time(fname):
	"""
	Given an FPGA summary file (summary.csv),
		return the performance number in ms for both scheduler and executor

	Key log:
	Device,Compute Unit,Kernel,Global Work Size,Local Work Size,Number Of Calls,Dataflow Execution,Max Overlapping Executions,Dataflow Acceleration,Total Time (ms),Minimum Time (ms),Average Time (ms),Maximum Time (ms),Clock Frequency (MHz),
	xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1,executor,1:1:1,1:1:1,1,Yes,1,1.000000x,37.9596,37.9596,37.9596,37.9596,200,
	xilinx_u250_gen3x16_xdma_shell_4_1-0,scheduler_1,scheduler,1:1:1,1:1:1,1,Yes,1,1.000000x,47.0463,47.0463,47.0463,47.0463,200,
	"""
	pattern = r"(\d+.\d+)" # float
	executor_keyword = 'xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1,executor,1:1:1,1:1:1,1,Yes,1,1.000000x,'
	scheduler_keyword = 'xilinx_u250_gen3x16_xdma_shell_4_1-0,scheduler_1,scheduler,1:1:1,1:1:1,1,Yes,1,1.000000x,'
	time_ms_executor = None
	time_ms_scheduler = None
	
	with open(fname) as f:
		lines = f.readlines()
		for line in lines:
			if executor_keyword in line:
				new_line = line.replace(executor_keyword, "")
				time_ms_executor = re.findall(pattern, new_line)[0]
				if '.' in time_ms_executor:
					time_ms_executor = float(time_ms_executor)
				else: # only has int part, e.g., 1146,1146
					time_ms_executor = float(re.findall(r"\d+", time_ms_executor)[0])
			elif scheduler_keyword in line:
				new_line = line.replace(scheduler_keyword, "")
				time_ms_scheduler = re.findall(pattern, new_line)[0]
				if '.' in time_ms_scheduler:
					time_ms_scheduler = float(time_ms_scheduler)
				else: # only has int part, e.g., 1146,1146
					time_ms_scheduler = float(re.findall(r"\d+", time_ms_scheduler)[0])
	assert time_ms_executor is not None
	assert time_ms_scheduler is not None 

	return time_ms_executor, time_ms_scheduler

if __name__ == '__main__':
    
	print("This script is only compatible to FPGA V3.1 and above which passes the page size as a parameter")
	# Init: clear up old bin, which is saved in the current dir
	tree_A_dir = 'tree_A.bin' # os.path.join(os.path.dirname(cpp_exe_dir), 'tree_A.bin')
	tree_B_dir = 'tree_B.bin' # os.path.join(os.path.dirname(cpp_exe_dir), 'tree_B.bin')
	print("Removing old tree bins...")
	os.system(f"rm {tree_A_dir}")
	os.system(f"rm {tree_B_dir}")

	# First execute the CPP
	log_cpp = 'log_cpp'
	cmd_cpp = f'{cpp_exe_dir} {C_file_A} {C_file_B} {max_entry_size} > {log_cpp}'
	print("Executing cpp command:\n", cmd_cpp)
	os.system(cmd_cpp)
	num_results = get_number_file_with_keywords(log_cpp, "Number of results:", "int")
	node_bytes = get_number_file_with_keywords(log_cpp, "Bytes per node", "int")
	time_ms_CPU = get_number_file_with_keywords(log_cpp, "Sync traversal duration:", "float")
	print("Number of results: ", num_results)
	print("CPU sync traversal time: {} ms".format(time_ms_CPU))

	# Second, get tree depth
	level_A = get_tree_depth_procedure("A", node_bytes)
	level_B = get_tree_depth_procedure("B", node_bytes)
	print("Level A: ", level_A)
	print("Level B: ", level_B)

	# Third, run FPGA
	for run_id in range(num_runs):
		"""
		std::cout << "Usage: " << argv[0] << "<1: xclbin>  <2: TreeBin Dir 1> <3: TreeBin Dir 2> <4: Tree 1 level> " 
		"<5: Tree 2 level> <6: Max entry num in a node>  <7: page_bytes> <8: num results>" << std::endl;
		"""
		host_full = os.path.join(FPGA_project_dir, FPGA_host_name)
		xclbin_full = os.path.join(FPGA_project_dir, FPGA_bin_name)
		tree_A_full = os.path.join(os.path.dirname(cpp_exe_dir), 'tree_A.bin')
		tree_B_full = os.path.join(os.path.dirname(cpp_exe_dir), 'tree_B.bin')
		log_FPGA = 'log_FPGA'
		cmd_FPGA = f"{host_full} {xclbin_full} {tree_A_dir} {tree_B_dir} {level_A} {level_B} {max_entry_size} {node_bytes} {num_results} > {log_FPGA}"
		print("Executing FPGA command:\n", cmd_FPGA)
		os.system(cmd_FPGA)
		
		assert assert_keywords_in_file(log_FPGA, "Result correct!") == True
		time_ms_FPGA_e2e = get_number_file_with_keywords(log_FPGA, "Duration (including memcpy out):", "float")
		time_ms_FPGA_kernel = get_number_file_with_keywords(log_FPGA, "Duration (kernel):", "float")
		# time_ms_executor, time_ms_scheduler = get_FPGA_summary_time(FPGA_log_name)
		# time_ms_FPGA_kernel = np.amax([time_ms_executor, time_ms_scheduler])
		print("Run {} FPGA end-to-end: {} ms".format(run_id, time_ms_FPGA_e2e))
		print("Run {} FPGA kernel: {} ms".format(run_id, time_ms_FPGA_kernel))