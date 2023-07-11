"""
This plot compares the effect of page size on the 16-PE FPGA,
    on various datasets and scales.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_FPGA_json, get_FPGA_mean_and_error_bar_dict, get_array_from_dict
plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot')

### TODO: add args for number of PEs

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


datasets = ["Uniform", "OSM"]
join_types = ["Point-in-Polygon", "Polygon-Polygon"]
size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

PE_num = 16
print("Number of PE: ", PE_num)
json_dict = load_FPGA_json(f'./json_FPGA/FPGA_perf_{PE_num}_PE_3_runs.json')
mean_and_error_bar_dict = get_FPGA_mean_and_error_bar_dict(json_dict)

def get_key_sets(dataset, join_type, max_entry_size, perf_metric="mean"):
    """
    perf_metric: {"mean", "std", "kernel_mean", "kernel_std"}
    """
    key_sets = [\
        [dataset, join_type, "100000", "100000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "100000", "1000000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "100000", "10000000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "1000000", "1000000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "1000000", "10000000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "10000000", "10000000", str(max_entry_size), perf_metric],\
    ]
    return key_sets

def plot_and_save(dataset, join_type):
	   
    y_mean_8 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 8, perf_metric="mean"))
    y_mean_16 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 16, perf_metric="mean"))
    y_mean_32 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 32, perf_metric="mean"))
    # y_mean_48 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 48, perf_metric="mean"))
    # y_mean_64 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 64, perf_metric="mean"))
    print(y_mean_8)
    print(y_mean_16)
    print(y_mean_32)
    # print(y_mean_48)
    # print(y_mean_64)

    y_std_8 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 8, perf_metric="std"))
    y_std_16 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 16, perf_metric="std"))
    y_std_32 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 32, perf_metric="std"))
    # y_std_48 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 48, perf_metric="std"))
    # y_std_64 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 64, perf_metric="std"))

    x_labels = ['100K x 100K', '100K x 1M', '100K x 10M', '1M x 1M', '1M x 10M', '10M x 10M']

    x = np.arange(len(x_labels))  # the label locations
    width = 0.2  # the width of the bars

    fig, ax = plt.subplots(1, 1, figsize=(8, 4))
    rects0  = ax.bar(x - 2 * width, y_mean_8, width)#, label='Men')
    rects1  = ax.bar(x - width, y_mean_16, width)#, label='Men')
    rects2  = ax.bar(x, y_mean_32, width)#, label='Men')
    # rects3  = ax.bar(x + width, y_mean_48, width)#, label='Men')
    # rects4 = ax.bar(x + 2 * width, y_mean_64, width)#, label='Women')

    ax.errorbar(x - 2 * width, y_mean_8, yerr=y_std_8, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x - width, y_mean_16, yerr=y_std_16, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x, y_mean_32, yerr=y_std_32, fmt=',', ecolor='black', capsize=5)
    # ax.errorbar(x + width, y_mean_48, yerr=y_std_48, fmt=',', ecolor='black', capsize=5)
    # ax.errorbar(x + 2 * width, y_mean_64, yerr=y_std_64, fmt=',', ecolor='black', capsize=5)

    label_font = 14
    legend_font = 11
    tick_font = 12

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Latency in Log Scale (ms)', fontsize=label_font)
    ax.set_xlabel('Dataset sizes', fontsize=label_font)
    ax.set_title(f'{dataset}, {join_type}', fontsize=label_font)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=tick_font)
    ax.legend([rects0, rects1, rects2], ["Max node size = 8", "Max node size = 16", "Max node size = 32"], loc="upper left", ncol=1, \
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


    autolabel(rects0)
    autolabel(rects1)
    autolabel(rects2)
    # autolabel(rects3)
    # autolabel(rects4)

    plt.yscale("log")
    ax.set(ylim=[0.5 * np.amin(y_mean_8 + y_mean_16 + y_mean_32), 10 * np.amax(y_mean_8 + y_mean_16 + y_mean_32)]) 
    plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/FPGA_page_size_{dataset}_{join_type}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

if __name__ == "__main__":

    for dataset in datasets:
        for join_type in join_types:
            plot_and_save(dataset, join_type)