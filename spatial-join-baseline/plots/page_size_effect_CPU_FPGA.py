"""
This plot compares the effect of page size on the CPP implementation
    on various datasets and scales.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_cpp_json, load_FPGA_json, get_cpp_mean_and_error_bar_dict, get_FPGA_mean_and_error_bar_dict, get_array_from_dict, compare_cpp_FPGA_num_results
# plt.style.use('seaborn-colorblind') 
plt.style.use('seaborn-pastel')
# plt.style.use('ggplot')
# plt.style.use('fivethirtyeight')

size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

cpu_json_dict = load_cpp_json(f'./json_cpp/CPU_perf_16_threads.json')
cpu_mean_and_error_bar_dict = get_cpp_mean_and_error_bar_dict(cpu_json_dict)


PE_num = 16
print("Number of PE: ", PE_num)
fpga_json_dict = load_FPGA_json(f'./json_FPGA/FPGA_perf_{PE_num}_PE_3_runs.json')
fpga_mean_and_error_bar_dict = get_FPGA_mean_and_error_bar_dict(fpga_json_dict)

legend_plot_set = None

# compare_cpp_FPGA_num_results(f'./json_cpu/static_bfs_multi.json', f'./json_FPGA/FPGA_perf_1_PE_5_runs.json')

def get_key_sets(dataset, join_type, perf_metric="mean"):
    """
    perf_metric: {"mean", "std", "kernel_mean", "kernel_std"}
    """
    key_sets = [\
        [dataset, join_type, "10000000", "10000000", str(8), perf_metric],\
        [dataset, join_type, "10000000", "10000000", str(16), perf_metric],\
        [dataset, join_type, "10000000", "10000000", str(32), perf_metric],\
    ]
    return key_sets


label_font = 12
markersize = 10
legend_font = 12
tick_font = 10

def subplot(dataset, join_type, ax):

    cpu_y_mean = get_array_from_dict(cpu_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="mean_bfs_dynamic_ms"))
    fpga_y_mean = get_array_from_dict(fpga_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="mean"))
    fpga_y_mean_kernel = get_array_from_dict(fpga_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="kernel_mean"))
    print(cpu_y_mean)
    print(fpga_y_mean)
    print(fpga_y_mean_kernel)

    cpu_y_std = get_array_from_dict(cpu_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="std_bfs_dynamic_ms"))
    fpga_y_std = get_array_from_dict(fpga_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="std"))
    fpga_y_std_kernel = get_array_from_dict(fpga_mean_and_error_bar_dict, get_key_sets(dataset, join_type, perf_metric="kernel_std"))

    x_labels = ["8", "16", "32"]
    x = np.arange(len(x_labels))  # the label locations

    
    plot_cpu = ax.plot(x, cpu_y_mean, marker='o', markersize=markersize)
    plot_fpga = ax.plot(x, fpga_y_mean, marker='^', markersize=markersize)
    plot_fpga_kernel = ax.plot(x, fpga_y_mean_kernel, marker='x', markersize=markersize)

    errorbar_size = 8    
    ax.errorbar(x, cpu_y_mean, yerr=cpu_y_std, fmt=',', ecolor='black', capsize=errorbar_size)
    ax.errorbar(x, fpga_y_mean, yerr=fpga_y_std, fmt=',', ecolor='black', capsize=errorbar_size)
    ax.errorbar(x, fpga_y_mean_kernel, yerr=fpga_y_std_kernel, fmt=',', ecolor='black', capsize=errorbar_size)

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Latency (ms)', fontsize=label_font)
    # ax.set_ylabel('Latency in Log Scale (ms)', fontsize=label_font)
    ax.set_xlabel('R-tree node sizes', fontsize=label_font)
    ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=tick_font)

    global legend_plot_set
    if legend_plot_set is None:
        legend_plot_set = [plot_cpu[0], plot_fpga[0], plot_fpga_kernel[0]]
        ax.legend([plot_cpu[0], plot_fpga[0], plot_fpga_kernel[0]], 
              ["C++ multi-thread", "SwiftSpatial (end-to-end)", "SwiftSpatial (kernel only)"], loc=(0.05, 0.25), ncol=1, \
                facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)


    # plt.yscale("log")
    # ax.set(ylim=[0.5 * np.amin(cpu_y_mean_16 + cpu_y_mean_32 + cpu_y_mean_64), 10 * np.amax(cpu_y_mean_16 + cpu_y_mean_32 + cpu_y_mean_64)]) 
    # plt.rcParams.update({'figure.autolayout': True})

    # plt.savefig(f'./images/page_size_effect_{dataset}_{join_type}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

def plot_and_save():

    datasets = ["Uniform", "OSM"]
    join_types = ["Point-in-Polygon", "Polygon-Polygon"]
    fig, ax_array = plt.subplots(1, 4, figsize=(16, 3.2))
    fig.tight_layout(pad=3.0)
    
    cnt = 0
    for dataset in datasets:
        for join_type in join_types:
            subplot(dataset, join_type, ax_array[cnt])
            cnt += 1

    # ax_array[0].legend(legend_plot_set, 
    #           ["C++ multi-thread", "FPGA (end-to-end)", "FPGA (kernel only)"], loc="upper left", ncol=3, \
    #         facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)


    # plt.yscale("log")
    # ax.set(ylim=[0.5 * np.amin(cpu_y_mean_16 + cpu_y_mean_32 + cpu_y_mean_64), 10 * np.amax(cpu_y_mean_16 + cpu_y_mean_32 + cpu_y_mean_64)]) 
    # plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/page_size_effect.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

if __name__ == "__main__":

    plot_and_save()
    # for dataset in datasets:
    #     for join_type in join_types:
    #         plot_and_save(dataset, join_type)