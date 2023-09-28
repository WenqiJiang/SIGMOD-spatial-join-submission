"""
Run single-thread tile-level join benchmark using nested loop VS plane-sweep

Example Usage: 
python cpp_run_tile_join_microbench.py \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data 

"""
import argparse 
import json
import os
from utils import get_number_file_with_keywords


parser = argparse.ArgumentParser()
parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench', help="the CPP exe file")
parser.add_argument('--C_file_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data', help="the CPP input file")
parser.add_argument('--log_name', type=str, default='cpu_microbench_log', help="output log")

args = parser.parse_args()
cpp_exe_dir = args.cpp_exe_dir
C_file_dir = args.C_file_dir
log_name = args.log_name

print("CPU benchmarking script")
print("------------------------------------------------------")

datasets = [
    # less populated (map size = 10000)
    ('C_tile_microbench_map_size_10000_polygon_file_0.txt', 'C_tile_microbench_map_size_10000_polygon_file_1.txt'), \
    # moderately populated (map size = 10)
    ('C_tile_microbench_map_size_10_polygon_file_0.txt', 'C_tile_microbench_map_size_10_polygon_file_1.txt'), \
    # highly populated (map size = 5)
    ('C_tile_microbench_map_size_5_polygon_file_0.txt', 'C_tile_microbench_map_size_5_polygon_file_1.txt'), \
            ]
tile_size_list = [4, 8, 16, 32, 64, 128]

    
def parse_perf_result(fname):
    """
    Get the performance of a single run from the cpp log
    Return: num_results, build_index_1_ms, build_index_2_ms, sync_traversal_ms, bfs_static_ms, bfs_dfs_static_ms, bfs_dynamic_ms, bfs_dfs_dynamic_ms
    """
        
    num_results_nested_loop = get_number_file_with_keywords(fname, 'Num Results (Nested Loop):', "int")
    num_results_plane_sweep = get_number_file_with_keywords(fname, 'Num Results (Stripe):', "int")
    per_tile_us_nested_loop = get_number_file_with_keywords(fname, 'Nested loop join time (per tile pair):', "float")
    per_tile_us_plane_sweep = get_number_file_with_keywords(fname, 'Plane sweep join time (per tile pair):', "float")

    return num_results_nested_loop, num_results_plane_sweep, per_tile_us_nested_loop, per_tile_us_plane_sweep

for dataset_A, dataset_B in datasets:
    print(f"\n\n===== Datasets: {dataset_A}, {dataset_B} =====")

    for tile_size in tile_size_list:
        print("\nTile size: ", tile_size)

        cmd = f'{cpp_exe_dir} {C_file_dir}/{dataset_A} {C_file_dir}/{dataset_B} {tile_size} > {log_name}'
        print("Executing: ", cmd)
        os.system(cmd)

        num_results_nested_loop, num_results_plane_sweep, per_tile_us_nested_loop, per_tile_us_plane_sweep = parse_perf_result(log_name)
        print("\tNum results (Nested Loop): {}\t(Plane Sweep): {}".format(num_results_nested_loop, num_results_plane_sweep))
        if num_results_nested_loop != num_results_plane_sweep:
            print("\t\Warning: num results not equal!")
        print("\tPer tile time in us (Nested Loop): {:.2f} us\t(Plane Sweep):  {:.2f} us".format(per_tile_us_nested_loop, per_tile_us_plane_sweep))


print("------------------------------------------------------")
print("CPU experiments complete!")
