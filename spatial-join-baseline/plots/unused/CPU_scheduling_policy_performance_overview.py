"""
This plot compares the effect of page size on the CPP implementation
    on various datasets and scales.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_CPU_json, load_FPGA_json, get_CPU_mean_and_error_bar_dict, get_FPGA_mean_and_error_bar_dict, get_array_from_dict, compare_CPU_FPGA_num_results
plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot')

datasets = ["Uniform", "OSM"]
join_types = ["Point-in-Polygon", "Polygon-Polygon"]
size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

thread_num = 64
print("Number of threads: ", thread_num)
static_bfs_json_dict = load_CPU_json(f'./json_cpu/static_bfs_multi.json')
static_bfs_mean_and_error_bar_dict = get_CPU_mean_and_error_bar_dict(static_bfs_json_dict)

static_bfs_dfs_json_dict = load_CPU_json(f'./json_cpu/static_bfs+dfs_multi.json')
static_bfs_dfs_mean_and_error_bar_dict = get_CPU_mean_and_error_bar_dict(static_bfs_dfs_json_dict)

dynamic_bfs_json_dict = load_CPU_json(f'./json_cpu/dynamic_bfs_multi.json')
dynamic_bfs_mean_and_error_bar_dict = get_CPU_mean_and_error_bar_dict(dynamic_bfs_json_dict)

dynamic_bfs_dfs_json_dict = load_CPU_json(f'./json_cpu/dynamic_bfs+dfs_multi.json')
dynamic_bfs_dfs_mean_and_error_bar_dict = get_CPU_mean_and_error_bar_dict(dynamic_bfs_dfs_json_dict)

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

def plot_and_save(dataset, join_type, max_node_size):
    key_sets = get_key_sets(dataset, join_type, max_node_size, perf_metric="mean")

    static_bfs_y_mean = get_array_from_dict(static_bfs_mean_and_error_bar_dict, key_sets)
    static_bfs_dfs_y_mean = get_array_from_dict(static_bfs_dfs_mean_and_error_bar_dict, key_sets)
    dynamic_bfs_y_mean = get_array_from_dict(dynamic_bfs_mean_and_error_bar_dict, key_sets)
    dynamic_bfs_dfs_y_mean = get_array_from_dict(dynamic_bfs_dfs_mean_and_error_bar_dict, key_sets)

    print(static_bfs_y_mean)
    print(static_bfs_dfs_y_mean)
    print(dynamic_bfs_y_mean)
    print(dynamic_bfs_dfs_y_mean)

    key_sets = get_key_sets(dataset, join_type, 16, perf_metric="std")
	
    static_bfs_y_std = get_array_from_dict(static_bfs_mean_and_error_bar_dict, key_sets)
    static_bfs_dfs_y_std = get_array_from_dict(static_bfs_dfs_mean_and_error_bar_dict, key_sets)
    dynamic_bfs_y_std = get_array_from_dict(dynamic_bfs_mean_and_error_bar_dict, key_sets)
    dynamic_bfs_dfs_y_std = get_array_from_dict(dynamic_bfs_dfs_mean_and_error_bar_dict, key_sets)

    x_labels = ['100K x 100K', '100K x 1M', '100K x 10M', '1M x 1M', '1M x 10M', '10M x 10M']

    x = np.arange(len(x_labels))  # the label locations
    width = 0.15  # the width of the bars

    fig, ax = plt.subplots(1, 1, figsize=(16, 6))
    rects_cpu_static_bfs  = ax.bar(x - 1.5 * width, static_bfs_y_mean, width)
    rects_cpu_static_bfs_dfs  = ax.bar(x - 0.5 * width, static_bfs_dfs_y_mean, width)
    rects_cpu_dynamic_bfs  = ax.bar(x + 0.5 * width, dynamic_bfs_y_mean, width)
    rects_cpu_dynamic_bfs_dfs  = ax.bar(x + 1.5 * width, dynamic_bfs_dfs_y_mean, width)

    ax.errorbar(x - 1.5 * width, static_bfs_y_mean, yerr=static_bfs_y_std, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x - 0.5 * width, static_bfs_dfs_y_mean, yerr=static_bfs_dfs_y_std, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x + 0.5 * width, dynamic_bfs_y_mean, yerr=dynamic_bfs_y_std, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x + 1.5 * width, dynamic_bfs_dfs_y_mean, yerr=dynamic_bfs_dfs_y_std, fmt=',', ecolor='black', capsize=5)

    label_font = 14
    legend_font = 11
    tick_font = 12

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Latency in Log Scale (ms)', fontsize=label_font)
    ax.set_xlabel('Dataset sizes', fontsize=label_font)
    ax.set_title(f'{dataset}, {join_type}', fontsize=label_font)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=tick_font)
    ax.legend([rects_cpu_static_bfs, rects_cpu_static_bfs_dfs, rects_cpu_dynamic_bfs, rects_cpu_dynamic_bfs_dfs], 
              ["Static BFS", "Static BFS+DFS", "Dynamic BFS", "Dynamic BFS+DFS"], loc="upper left", ncol=2, \
    facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)


    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{:.2e}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, 1.05 * height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', rotation=90)


    autolabel(rects_cpu_static_bfs)
    autolabel(rects_cpu_static_bfs_dfs)
    autolabel(rects_cpu_dynamic_bfs)
    autolabel(rects_cpu_dynamic_bfs_dfs)

    plt.yscale("log")
    # ax.set(ylim=[0.5 * np.amin(static_bfs_y_mean + static_bfs_y_mean_32 + static_bfs_y_mean_64), 10 * np.amax(static_bfs_y_mean + static_bfs_y_mean_32 + static_bfs_y_mean_64)]) 
    plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/CPU_scheduling_policy_{dataset}_{join_type}_max_node_size{max_node_size}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

if __name__ == "__main__":

    for dataset in datasets:
        for join_type in join_types:
            for max_node_size in [16, 32, 64]:
            	plot_and_save(dataset, join_type, max_node_size)