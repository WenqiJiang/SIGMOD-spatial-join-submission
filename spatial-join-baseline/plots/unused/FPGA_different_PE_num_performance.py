"""
This plot compares the performance of different PE numbers on various node sizes. 
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_FPGA_json, get_FPGA_mean_and_error_bar_dict, get_array_from_dict
plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot')

# parser = argparse.ArgumentParser()
# parser.add_argument('--FPGA_project_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE')
# parser.add_argument('--FPGA_host_name', type=str, default='host', help="the name of the exe of the FPGA host")
# parser.add_argument('--FPGA_bin_name', type=str, default='xclbin/vadd.hw.xclbin', help="the name (as well as the subdir) of the FPGA bitstream")
# parser.add_argument('--FPGA_log_name', type=str, default='summary.csv', help="the name of the FPGA perf summary file")
# parser.add_argument('--cpp_exe_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/cpp/a.out', help="the CPP exe file")
# parser.add_argument('--C_file_A', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_0_set_0.txt', help="the CPP input file")
# parser.add_argument('--C_file_B', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_1_set_0.txt', help="the CPP input file")
# parser.add_argument('--get_tree_depth_py_dir', type=str, default='/mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py', help="the get tree depth file dir")
# parser.add_argument('--max_entry_size', type=int, default=32, help="the max entry numbers in an R tree node")
# parser.add_argument('--num_runs', type=int, default=1, help="number of FPGA runs")

# args = parser.parse_args()
# FPGA_project_dir = args.FPGA_project_dir
# FPGA_host_name = args.FPGA_host_name
# FPGA_bin_name = args.FPGA_bin_name


PE_nums = [1, 2, 4, 8, 16]
json_dict_N_PE = dict()
mean_and_error_bar_dict_N_PE = dict()
for PE_num in PE_nums:
    json_dict_N_PE[PE_num] = load_FPGA_json(f'./json_FPGA/FPGA_perf_{PE_num}_PE_5_runs.json')
    mean_and_error_bar_dict_N_PE[PE_num] = get_FPGA_mean_and_error_bar_dict(json_dict_N_PE[PE_num])


# Use 10M x 10M Uniform Polygon Join
for dataset in ["Uniform", "OSM"]:
    for join_type in ["Polygon-Polygon", "Point-in-Polygon"]:
        def get_y_mean_std_array_with_various_PE_num(max_entry_size):
            y_mean_array = []
            y_std_array = []
            for PE_num in PE_nums:
                key_sets = [ [dataset, join_type, "10000000", "10000000", str(max_entry_size), "mean"]]
                y_mean = get_array_from_dict(mean_and_error_bar_dict_N_PE[PE_num], key_sets)
                y_mean_array += y_mean
                key_sets = [ [dataset, join_type, "10000000", "10000000", str(max_entry_size), "std"]]
                y_std = get_array_from_dict(mean_and_error_bar_dict_N_PE[PE_num], key_sets)
                y_std_array += y_std
            return y_mean_array, y_std_array

        y_mean_16, y_std_16 = get_y_mean_std_array_with_various_PE_num(16)
        y_mean_32, y_std_32 = get_y_mean_std_array_with_various_PE_num(32)
        y_mean_64, y_std_64 = get_y_mean_std_array_with_various_PE_num(64)

        x_labels = ['1', '2', '4', '8', '16']

        x = np.arange(len(x_labels))  # the label locations
        width = 0.2  # the width of the bars

        fig, ax = plt.subplots(1, 1, figsize=(8, 3))
        rects1  = ax.bar(x - width, y_mean_16, width)#, label='Men')
        rects2  = ax.bar(x, y_mean_32, width)#, label='Men')
        rects3 = ax.bar(x + width, y_mean_64, width)#, label='Women')

        ax.errorbar(x - width, y_mean_16, yerr=y_std_16, fmt=',', ecolor='black', capsize=5)
        ax.errorbar(x, y_mean_32, yerr=y_std_32, fmt=',', ecolor='black', capsize=5)
        ax.errorbar(x + width, y_mean_64, yerr=y_std_64, fmt=',', ecolor='black', capsize=5)

        label_font = 14
        legend_font = 10
        tick_font = 12

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('Latency in Log Scale (ms)', fontsize=label_font)
        ax.set_xlabel('Number of join units', fontsize=label_font)
        ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, fontsize=tick_font)
        ax.legend([rects1, rects2, rects3], ["Max node size = 16", "Max node size = 32", "Max node size = 64"], loc="upper center", ncol=3, \
        facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)


        def autolabel(rects):
            """Attach a text label above each bar in *rects*, displaying its height."""
            for rect in rects:
                height = rect.get_height()
                ax.annotate('{:.1e}'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, 1.05 * height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom', rotation=90)


        autolabel(rects1)
        autolabel(rects2)
        autolabel(rects3)

        plt.yscale("log")
        ax.set(ylim=[0.5 * np.amin(y_mean_16 + y_mean_32 + y_mean_64), 10 * np.amax(y_mean_16 + y_mean_32 + y_mean_64)]) 
        plt.rcParams.update({'figure.autolayout': True})

        plt.savefig(f'./images/FPGA_Perf_different_PE_num_{dataset}_{join_type}.png', transparent=False, dpi=200, bbox_inches="tight")
        # plt.show()
