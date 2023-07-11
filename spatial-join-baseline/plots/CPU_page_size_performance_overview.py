"""
This plot compares the effect of page size on the CPP implementation
    on various datasets and scales.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_cpp_json, get_cpp_mean_and_error_bar_dict, get_array_from_dict, compare_cpp_FPGA_num_results
plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot')

datasets = ["Uniform", "OSM"]
join_types = ["Point-in-Polygon", "Polygon-Polygon"]
size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

thread_num = 64
print("Number of threads: ", thread_num)
json_dict = load_cpp_json(f'./json_cpp/CPU_perf_16_threads.json')
mean_and_error_bar_dict = get_cpp_mean_and_error_bar_dict(json_dict)

# compare_cpp_FPGA_num_results(f'./json_cpu/CPU_perf_16_threads.json', f'./json_FPGA/FPGA_perf_1_PE_5_runs.json')

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
    y_mean_8 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 8, perf_metric="mean_bfs_dynamic_ms"))
    y_mean_16 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 16, perf_metric="mean_bfs_dynamic_ms"))
    y_mean_32 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 32, perf_metric="mean_bfs_dynamic_ms"))
    # y_mean_64 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 64, perf_metric="mean_bfs_dynamic_ms"))
    print(y_mean_8)
    print(y_mean_16)
    print(y_mean_32)
    # print(y_mean_64)

    y_std_8 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 8, perf_metric="std_bfs_dynamic_ms"))
    y_std_16 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 16, perf_metric="std_bfs_dynamic_ms"))
    y_std_32 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 32, perf_metric="std_bfs_dynamic_ms"))
    # y_std_64 = get_array_from_dict(mean_and_error_bar_dict, get_key_sets(dataset, join_type, 64, perf_metric="std_bfs_dynamic_ms"))

    x_labels = ['100K x 100K', '100K x 1M', '100K x 10M', '1M x 1M', '1M x 10M', '10M x 10M']

    x = np.arange(len(x_labels))  # the label locations
    width = 0.2  # the width of the bars

    fig, ax = plt.subplots(1, 1, figsize=(8, 4))
    rects0  = ax.bar(x - 2 * width, y_mean_8, width)#, label='Men')
    rects1  = ax.bar(x - width, y_mean_16, width)#, label='Men')
    rects2  = ax.bar(x, y_mean_32, width)#, label='Men')
    # rects3 = ax.bar(x + width, y_mean_64, width)#, label='Women')

    ax.errorbar(x - 2 * width, y_mean_8, yerr=y_std_8, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x - width, y_mean_16, yerr=y_std_16, fmt=',', ecolor='black', capsize=5)
    ax.errorbar(x, y_mean_32, yerr=y_std_32, fmt=',', ecolor='black', capsize=5)
    # ax.errorbar(x + width, y_mean_64, yerr=y_std_64, fmt=',', ecolor='black', capsize=5)

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

    plt.yscale("log")
    ax.set(ylim=[0.5 * np.amin(y_mean_8 + y_mean_16 + y_mean_32), 10 * np.amax(y_mean_8 + y_mean_16 + y_mean_32)]) 
    plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/CPU_page_size_{dataset}_{join_type}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

if __name__ == "__main__":

    for dataset in datasets:
        for join_type in join_types:
            plot_and_save(dataset, join_type)