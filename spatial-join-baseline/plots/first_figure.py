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
# plt.style.use('seaborn-deep')
# plt.style.use('seaborn-pastel')
# plt.style.use('fivethirtyeight')

def get_default_colors():

  default_colors = []
  for i, color in enumerate(plt.rcParams['axes.prop_cycle']):
      default_colors.append(color["color"])
      print(color["color"], type(color["color"]))

  return default_colors

datasets = ["OSM", "Uniform"]
join_types = ["Polygon-Polygon", "Point-in-Polygon"]
size_scales = [int(1e5), int(1e6), int(1e7)] # measure 100K~1M

fpga_json_dict = load_json(f'./json_FPGA/FPGA_perf_16_PE_3_runs.json')
fpga_mean_and_error_bar_dict = get_FPGA_mean_and_error_bar_dict(fpga_json_dict)

cpp_mt_json_dict = load_json(f'./json_cpp/CPU_perf_16_threads.json')
cpp_mt_mean_and_error_bar_dict = get_cpp_mean_and_error_bar_dict(cpp_mt_json_dict)

cpp_st_json_dict = load_json(f'./json_cpp/CPU_perf_singlethread_sync_traversal.json')
cpp_st_mean_and_error_bar_dict = get_cpp_mean_and_error_bar_dict(cpp_st_json_dict)

postgis_json_dict = load_json(f'./json_postgis/postgis_runs.json')
postgis_mean_and_error_bar_dict = get_software_baseline_error_bar_dict(postgis_json_dict)

sedona_json_dict = load_sedona_json(f'./json_sedona/sedona_runs.json')
sedona_mean_and_error_bar_dict = get_software_baseline_error_bar_dict(sedona_json_dict)

spatialspark_json_dict = load_json(f'./json_spatialspark/spatialspark_runs.json')
spatialspark_mean_and_error_bar_dict = get_spatialspark_mean_and_error_bar_dict(spatialspark_json_dict)

cuspatial_json_dict = load_json(f'./json_cuspatial/GPU_perf_3_runs_A100_40GB_SXM_batch_10K.json')
cuspatial_mean_and_error_bar_dict = get_software_baseline_error_bar_dict(cuspatial_json_dict)


def get_key_sets_with_max_entry(dataset, join_type, max_entry_size, perf_metric="mean"):
    """
    perf_metric: {"mean", "std", "kernel_mean", "kernel_std"}
    """
    key_sets = [\
        # [dataset, join_type, "100000", "100000", str(max_entry_size), perf_metric],\
        # [dataset, join_type, "100000", "1000000", str(max_entry_size), perf_metric],\
        # [dataset, join_type, "100000", "10000000", str(max_entry_size), perf_metric],\
        # [dataset, join_type, "1000000", "1000000", str(max_entry_size), perf_metric],\
        # [dataset, join_type, "1000000", "10000000", str(max_entry_size), perf_metric],\
        [dataset, join_type, "10000000", "10000000", str(max_entry_size), perf_metric],\
    ]
    return key_sets

def get_key_sets_without_max_entry(dataset, join_type, perf_metric="mean"):
    """
    perf_metric: {"mean", "std", "kernel_mean", "kernel_std"}
    """
    key_sets = [\
        # [dataset, join_type, "100000", "100000", perf_metric],\
        # [dataset, join_type, "100000", "1000000", perf_metric],\
        # [dataset, join_type, "100000", "10000000", perf_metric],\
        # [dataset, join_type, "1000000", "1000000", perf_metric],\
        # [dataset, join_type, "1000000", "10000000", perf_metric],\
        [dataset, join_type, "10000000", "10000000", perf_metric],\
    ]
    return key_sets


def plot_and_save():

    fpga_y_mean = []
    cpp_mt_y_mean = []
    cpp_st_y_mean = []
    postgis_y_mean = []
    sedona_y_mean = []
    spatialspark_y_mean = []
    cuspatial_y_mean = []
    
    for dataset in datasets:
        for join_type in join_types:
            key_sets_fpga = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean")
            key_sets_cpp_mt = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_bfs_dynamic_ms")
            key_sets_cpp_st = get_key_sets_with_max_entry(dataset, join_type, 16, perf_metric="mean_sync_traversal_ms")
            
            key_sets_postgis = get_key_sets_without_max_entry(dataset, join_type, perf_metric="mean_join_time_ms")
            key_sets_sedona = get_key_sets_without_max_entry(dataset, join_type, perf_metric="mean_join_time_ms")
            key_sets_spatialspark = get_key_sets_with_max_entry(dataset, join_type, 64, perf_metric="mean_join_time_ms")
            if join_type == 'Point-in-Polygon':
                key_sets_cuspatial = get_key_sets_without_max_entry(dataset, join_type, perf_metric="mean_join_time_ms")

            fpga_y_mean += get_array_from_dict(fpga_mean_and_error_bar_dict, key_sets_fpga)
            cpp_mt_y_mean += get_array_from_dict(cpp_mt_mean_and_error_bar_dict, key_sets_cpp_mt)
            cpp_st_y_mean += get_array_from_dict(cpp_st_mean_and_error_bar_dict, key_sets_cpp_st)
            postgis_y_mean += get_array_from_dict(postgis_mean_and_error_bar_dict, key_sets_postgis)
            sedona_y_mean += get_array_from_dict(sedona_mean_and_error_bar_dict, key_sets_sedona)
            spatialspark_y_mean += get_array_from_dict(spatialspark_mean_and_error_bar_dict, key_sets_spatialspark)
            if join_type == 'Point-in-Polygon':
                cuspatial_y_mean += get_array_from_dict(cuspatial_mean_and_error_bar_dict, key_sets_cuspatial)
            else:
                cuspatial_y_mean += [0]

    print(f"===== {dataset}, {join_type} starts =====")
    speedup_baseline = np.array(fpga_y_mean) / np.array(fpga_y_mean)
    speedup_mt = np.array(cpp_mt_y_mean) / np.array(fpga_y_mean)
    speedup_st = np.array(cpp_st_y_mean) / np.array(fpga_y_mean)
    speedup_postgis = np.array(postgis_y_mean) / np.array(fpga_y_mean)
    speedup_sedona = np.array(sedona_y_mean) / np.array(fpga_y_mean)
    speedup_spatialspark = np.array(spatialspark_y_mean) / np.array(fpga_y_mean)
    speedup_cuspatial = np.array(cuspatial_y_mean) / np.array(fpga_y_mean)
    print("Speedup over CPP MT: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_mt), np.amax(speedup_mt), speedup_mt))
    print("Speedup over CPP ST: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_st), np.amax(speedup_st), speedup_st))
    print("Speedup over PostGIS: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_postgis), np.amax(speedup_postgis), speedup_postgis))
    print("Speedup over Sedona: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_sedona), np.amax(speedup_sedona), speedup_sedona))
    print("Speedup over Spatial Spark: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_spatialspark), np.amax(speedup_spatialspark), speedup_spatialspark))
    print("Speedup over Spatial Spark: {:.2f} ~ {:.2f} X, {}".format(np.amin(speedup_cuspatial), np.amax(speedup_cuspatial), speedup_cuspatial))
    print(f"===== {dataset}, {join_type} finishes =====")

    x_labels = []
    for dataset in datasets:
        for join_type in join_types: 
            if join_type == 'Point-in-Polygon':
                x_labels.append(f"{dataset} 10Mx10M\nPoint-Polygon")
            else:
                x_labels.append(f"{dataset} 10Mx10M\n{join_type}")

    x = np.arange(len(x_labels))  # the label locations
    width = 0.1  # the width of the bars

    fig, ax = plt.subplots(1, 1, figsize=(9, 3))
    rects_fpga  = ax.bar(x - 2.5 * width, speedup_baseline, width)
    rects_cpp_mt  = ax.bar(x - 1.5 * width, speedup_mt, width)
    rects_cpp_st = ax.bar(x - 0.5 * width, speedup_st, width)
    rects_postgis  = ax.bar(x + 0.5 * width, speedup_postgis, width)
    rects_sedona  = ax.bar(x + 1.5 * width, speedup_sedona, width)
    rects_spatialspark = ax.bar(x + 2.5 * width, speedup_spatialspark, width)
    rects_cuspatial = ax.bar(x + 3.5 * width, speedup_cuspatial, width)

    label_font = 14
    text_font = 13
    legend_font = 11
    tick_font = 12

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Normalized Latency', fontsize=label_font)
    # ax.set_xlabel(f'{dataset}, {join_type}', fontsize=label_font + 1)
    # ax.set_xlabel(f'Dataset sizes', fontsize=label_font)
    # ax.set_title(f'{dataset}, {join_type}', fontsize=label_font)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=tick_font, color='black')

    ax.legend([rects_fpga, rects_cpp_mt, rects_cpp_st, rects_postgis, rects_sedona, rects_spatialspark, rects_cuspatial], 
            ["SwiftSpatial (FPGA)", "C++ Multi-thread", "C++ Single-thread", "PostGIS", "Sedona", "SpatialSpark", "cuSpatial (GPU)"], loc=(-0.11, 1.03), ncol=4, \
            facecolor='white', framealpha=1, frameon=False, fontsize=legend_font)

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            if height > 0:
                ax.annotate('{:.1f}x'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, 1.05 * height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom', rotation=90, fontsize=12)

    autolabel(rects_fpga)
    autolabel(rects_cpp_mt)
    autolabel(rects_cpp_st)
    autolabel(rects_postgis)
    autolabel(rects_sedona)
    autolabel(rects_spatialspark)
    autolabel(rects_cuspatial)

    plt.yscale("log")
    ax.set(ylim=[0.1, 30 * np.amax(speedup_cuspatial)]) 
    # ax.text(-0.4, 40 * np.amax(concatenated_array), f"{dataset}, {join_type}", fontsize=label_font + 2)
    concat_array = list(speedup_mt) + list(speedup_st) + list(speedup_postgis) + \
        list(speedup_sedona) + list(speedup_spatialspark) + list(speedup_cuspatial)
    ax.text(-0.4, 10 * np.amax(speedup_cuspatial), "Speedup: {:.1f}~{:.1f}x".format(np.amin(speedup_mt), 
        np.amax(concat_array)), fontsize=text_font, color=get_default_colors()[0], weight='bold')
    # plt.rcParams.update({'figure.autolayout': True})
    plt.grid(color='#cccccc', linestyle='-', linewidth=1)
    ax.xaxis.grid(False)
    ax.yaxis.grid(True)
    ax.set(facecolor = "white")

    plt.savefig(f'./images/first_figure.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()

if __name__ == "__main__":

    plot_and_save()
