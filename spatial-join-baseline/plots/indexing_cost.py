"""
This plot compares the effect of page size on the CPP implementation
    on various datasets and scales.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
from utils import load_json, load_sedona_json, get_cpp_mean_and_error_bar_dict, \
    get_FPGA_mean_and_error_bar_dict, get_spatialspark_mean_and_error_bar_dict, get_software_baseline_error_bar_dict, \
    get_array_from_dict, compare_cpp_FPGA_num_results
# plt.style.use('seaborn-colorblind') 
plt.style.use('ggplot')

datasets = ["Uniform", "OSM"]
join_types = ["Point-in-Polygon", "Polygon-Polygon"]
size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

fpga_json_dict = load_json(f'./json_FPGA/FPGA_perf_16_PE_3_runs.json')
fpga_mean_and_error_bar_dict = get_FPGA_mean_and_error_bar_dict(fpga_json_dict)

cpp_mt_json_dict = load_json(f'./json_cpp/CPU_perf_16_threads.json')
cpp_mt_mean_and_error_bar_dict = get_cpp_mean_and_error_bar_dict(cpp_mt_json_dict)

speedup_mt_min, speedup_mt_max = 10000000, 0
speedup_index_min, speedup_index_max = 10000000, 0

def update_min_max(old_min, old_max, new_min, new_max):
    """
    if new min and new max expands the old boundaries, update the old
    """
    if new_min < old_min:
        old_min = new_min
    if new_max > old_max:
        old_max = new_max
    return old_min, old_max

### TODO: cuspatial ###


def get_key_sets_with_max_entry(dataset, join_type, max_entry_size, perf_metric="mean"):
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

def get_key_sets_without_max_entry(dataset, join_type, perf_metric="mean"):
    """
    perf_metric: {"mean", "std", "kernel_mean", "kernel_std"}
    """
    key_sets = [\
        [dataset, join_type, "100000", "100000", perf_metric],\
        [dataset, join_type, "100000", "1000000", perf_metric],\
        [dataset, join_type, "100000", "10000000", perf_metric],\
        [dataset, join_type, "1000000", "1000000", perf_metric],\
        [dataset, join_type, "1000000", "10000000", perf_metric],\
        [dataset, join_type, "10000000", "10000000", perf_metric],\
    ]
    return key_sets


def plot_and_save(dataset, join_type):
    key_sets_fpga = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean")
    key_sets_cpp_mt = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_bfs_dynamic_ms")
    # key_sets_index_1 = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_build_index_1_ms")
    # key_sets_index_2 = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_build_index_2_ms")
    key_sets_index = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_build_both_indexes_ms")
    
    fpga_y_mean = get_array_from_dict(fpga_mean_and_error_bar_dict, key_sets_fpga)
    cpp_mt_y_mean = get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_cpp_mt)
    # index_1_y_mean = get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_index_1)
    # index_2_y_mean = get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_index_2)
    index_y_mean = get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_index)


    print(f"===== {dataset}, {join_type} starts =====")

    print("Key sets: ", key_sets_fpga)
    print("FPGA time: ", fpga_y_mean)
    print("C++ multi-thread time: ", cpp_mt_y_mean)
    print("Index time: ", index_y_mean)

    speedup_mt = np.array(cpp_mt_y_mean) / np.array(fpga_y_mean)
    speedup_index = np.array(index_y_mean) / np.array(fpga_y_mean)
    
    print("Speedup over CPP MT: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_mt), np.amax(speedup_mt), speedup_mt))
    print("Speedup over indexing: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_index), np.amax(speedup_index), speedup_index))
    print(f"===== {dataset}, {join_type} finishes =====")

    global speedup_mt_min, speedup_mt_max, speedup_index_min, speedup_index_max
    speedup_mt_min, speedup_mt_max = update_min_max(speedup_mt_min, speedup_mt_max, np.amin(speedup_mt), np.amax(speedup_mt))
    speedup_index_min, speedup_index_max = update_min_max(speedup_index_min, speedup_index_max, np.amin(speedup_index), np.amax(speedup_index))

    # key_sets_fpga = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="std")
    # key_sets_cpp_mt = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="std_bfs_dynamic_ms")

    # fpga_y_std = get_array_from_dict(fpga_mean_and_error_bar_dict, key_sets_fpga)
    # cpp_mt_y_std = get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_cpp_mt)

    # x_labels = ['100K x 100K', '100K x 1M', '100K x 10M', '1M x 1M', '1M x 10M', '10M x 10M']

    # x = np.arange(len(x_labels))  # the label locations
    # width = 0.1  # the width of the bars

    # fig, ax = plt.subplots(1, 1, figsize=(16, 3))
    # rects_fpga  = ax.bar(x - 2.5 * width, fpga_y_mean, width)
    # rects_cpp_mt  = ax.bar(x - 1.5 * width, cpp_mt_y_mean, width)
    

    # ax.errorbar(x - 2.5 * width, fpga_y_mean, yerr=fpga_y_std, fmt=',', ecolor='black', capsize=5)
    # ax.errorbar(x - 1.5 * width, cpp_mt_y_mean, yerr=cpp_mt_y_std, fmt=',', ecolor='black', capsize=5)
    
    # label_font = 13
    # legend_font = 11
    # tick_font = 12

    # # Add some text for labels, title and custom x-axis tick labels, etc.
    # ax.set_ylabel('Latency in Log Scale (ms)', fontsize=label_font)
    # ax.set_xlabel(f'{dataset}, {join_type}', fontsize=label_font + 1)
    # # ax.set_xlabel(f'Dataset sizes', fontsize=label_font)
    # # ax.set_title(f'{dataset}, {join_type}', fontsize=label_font)
    # ax.set_xticks(x)
    # ax.set_xticklabels(x_labels, fontsize=tick_font)
    # if join_type == 'Point-in-Polygon':
    #     ax.legend([rects_fpga, rects_cpp_mt, rects_cpp_st, rects_postgis, rects_sedona, rects_spatialspark, rects_cuspatial], 
    #             ["SwiftSpatial (FPGA)", "C++ Multi-thread", "C++ Single-thread", "PostGIS", "Sedona", "SpatialSpark", "cuSpatial (GPU)"], loc=(-0.01, 1.02), ncol=7, \
    #             facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)
    # else:
    #     ax.legend([rects_fpga, rects_cpp_mt, rects_cpp_st, rects_postgis, rects_sedona, rects_spatialspark], 
    #             ["SwiftSpatial (FPGA)", "C++ Multi-thread", "C++ Single-thread", "PostGIS", "Sedona", "SpatialSpark"], loc=(0.05, 1.02), ncol=6, \
    #             facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)

    # def autolabel(rects):
    #     """Attach a text label above each bar in *rects*, displaying its height."""
    #     for rect in rects:
    #         height = rect.get_height()
    #         ax.annotate('{:.1e}'.format(height),
    #                     xy=(rect.get_x() + rect.get_width() / 2, 1.05 * height),
    #                     xytext=(0, 3),  # 3 points vertical offset
    #                     textcoords="offset points",
    #                     ha='center', va='bottom', rotation=90)


    # def autolabel_with_speedup(rects, speedup_array):
    #     """Attach a text label above each bar in *rects*, displaying its height."""
    #     for i, rect in enumerate(rects):
    #         height = rect.get_height()
    #         ax.annotate('{:.1e} ({:.2f}x)'.format(height, speedup_array[i]),
    #                     xy=(rect.get_x() + rect.get_width() / 2, 1.05 * height),
    #                     xytext=(0, 3),  # 3 points vertical offset
    #                     textcoords="offset points",
    #                     ha='center', va='bottom', rotation=90)
            
    # autolabel(rects_fpga)
    # # autolabel_with_speedup(rects_cpp_mt, speedup_mt)
    # autolabel(rects_cpp_mt)

    # plt.yscale("log")
    # # ax.set(ylim=[0.5 * np.amin(cpp_mt_y_mean_16 + cpp_mt_y_mean_32 + cpp_mt_y_mean_64), 10 * np.amax(cpp_mt_y_mean_16 + cpp_mt_y_mean_32 + cpp_mt_y_mean_64)]) 
    # if join_type == 'Point-in-Polygon':
    #     concatenated_array = fpga_y_mean + cpp_mt_y_mean + cpp_st_y_mean + postgis_y_mean + sedona_y_mean + spatialspark_y_mean + cuspatial_y_mean
    # else:
    #     concatenated_array = fpga_y_mean + cpp_mt_y_mean + cpp_st_y_mean + postgis_y_mean + sedona_y_mean + spatialspark_y_mean
    # ax.set(ylim=[0.5 * np.amin(concatenated_array), 500 * np.amax(concatenated_array)]) 
    # # ax.text(-0.4, 40 * np.amax(concatenated_array), f"{dataset}, {join_type}", fontsize=label_font + 2)
    # ax.text(-0.4, 40 * np.amax(concatenated_array), "Speedup over C++ multi-thread: {:.2f}~{:.2f}x".format(np.amin(speedup_mt), np.amax(speedup_mt)), fontsize=label_font)
    # # plt.rcParams.update({'figure.autolayout': True})

    # plt.savefig(f'./images/all_system_performance_{dataset}_{join_type}.png', transparent=False, dpi=200, bbox_inches="tight")
    # # plt.show()

if __name__ == "__main__":

    for dataset in datasets:
        for join_type in join_types:
            plot_and_save(dataset, join_type)
    

    print(f"===== Speedup over all datasets =====")
    print("Speedup over C++ multi-thread: {:.2f} ~ {:.2f} X".format(speedup_mt_min, speedup_mt_max))
    print("Speedup over indexing: {:.2f} ~ {:.2f} X".format(speedup_index_min, speedup_index_max))
    