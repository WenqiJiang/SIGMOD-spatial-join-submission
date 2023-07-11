"""
Show performance scalabiility given different PE num
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import MaxNLocator

from utils import load_FPGA_json, get_FPGA_mean_and_error_bar_dict, get_array_from_dict
# plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot') 
plt.style.use('seaborn-pastel')
# plt.style.use('fivethirtyeight')

PE_nums = [1, 2, 4, 8, 16]
json_dict_N_PE = dict()
mean_and_error_bar_dict_N_PE = dict()
for PE_num in PE_nums:
    json_dict_N_PE[PE_num] = load_FPGA_json(f'./json_FPGA/FPGA_perf_{PE_num}_PE_3_runs.json')
    mean_and_error_bar_dict_N_PE[PE_num] = get_FPGA_mean_and_error_bar_dict(json_dict_N_PE[PE_num])


def plot_and_save_speedup(dataset, join_type, time='e2e'):

    print(f"===== {dataset}\t{join_type}\t{time}\t =====")
    
    assert time in ["e2e", "kernel"] 
    def get_y_mean_array_with_various_PE_num(max_entry_size, time):
        y_mean_array = []
        for PE_num in PE_nums:
            if time == 'e2e':
                key_sets = [[dataset, join_type, "10000000", "10000000", str(max_entry_size), "mean"]]
            elif time == 'kernel':
                key_sets = [[dataset, join_type, "10000000", "10000000", str(max_entry_size), "kernel_mean"]]
            y_mean = get_array_from_dict(mean_and_error_bar_dict_N_PE[PE_num], key_sets)
            y_mean_array += y_mean
        return y_mean_array
    
    y_mean_8 = get_y_mean_array_with_various_PE_num(8, time)
    y_mean_16 = get_y_mean_array_with_various_PE_num(16, time)
    y_mean_32 = get_y_mean_array_with_various_PE_num(32, time)
    # y_mean_64 = get_y_mean_array_with_various_PE_num(64)
    print(y_mean_8)
    print(y_mean_16)
    print(y_mean_32)
    # print(y_mean_64)

    def get_speedup_array(array):
        """
        Given an time array of different PE num, e.g. [10, 5, 2.5, 1.25]
            compute the speedup array, e.g., [1, 2, 4, 8]
        """
        baseline = array[0]
        speedup_array = []
        for a in array:
            speedup_array.append(baseline / a)
        return speedup_array
    
    y_speedup_8 = get_speedup_array(y_mean_8)
    y_speedup_16 = get_speedup_array(y_mean_16)
    y_speedup_32 = get_speedup_array(y_mean_32)
    # y_speedup_64 = get_speedup_array(y_mean_64)
    print(y_speedup_8)
    print(y_speedup_16)
    print(y_speedup_32)
    # print(y_speedup_64)
    y_linear_speedup = [1, 2, 4, 8, 16]

    fig, ax = plt.subplots(1, 1, figsize=(4, 4))

    label_font = 12
    markersize = 10
    legend_font = 12
    tick_font = 10

    plot_8 = ax.plot(PE_nums, y_speedup_8, marker='o', markersize=markersize)
    plot_16 = ax.plot(PE_nums, y_speedup_16, marker='^', markersize=markersize)
    plot_32 = ax.plot(PE_nums, y_speedup_32, marker='x', markersize=markersize)
    # plot_64 = ax.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
       
    plot_linear = ax.plot(PE_nums, y_linear_speedup, linestyle='dashed', color='grey')
    ax.text(5, 5 + 1.2, "Linear speedup", rotation=45, fontsize=label_font)
    
    if dataset == 'Uniform' and time == 'kernel':
        ax.annotate("Mem. bound (read)", xy=(PE_nums[-1], y_speedup_8[-1]), xytext=(PE_nums[-2], y_speedup_8[-1] + 2), arrowprops={"arrowstyle": '-|>', 'color': '#1f1f1f', 'linewidth': 2}, fontsize=legend_font)
    if dataset == 'OSM' and time == 'kernel':
        ax.annotate("Mem. bound (write)", xy=(PE_nums[-1], y_speedup_32[-1]), xytext=(PE_nums[-2], y_speedup_32[-1] + 2), arrowprops={"arrowstyle": '-|>', 'color': '#1f1f1f', 'linewidth': 2}, fontsize=legend_font)
    
    ax.legend([plot_8[0], plot_16[0], plot_32[0]], ["Node size = 8", "Node size = 16", "Node size = 32"], loc='upper left', frameon=False, fontsize=legend_font)
    # ax.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
    # ax.get_xaxis().set_visible(True)
    ax.set_xlabel('Number of join units', fontsize=label_font)
    if time == 'e2e':
        ax.set_ylabel('Speedup (end-to-end)', fontsize=label_font)
    elif time == 'kernel':
        ax.set_ylabel('Speedup (FPGA kernel only)', fontsize=label_font)

    ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

    ax.set(xlim=[0, 17], ylim=[0, 17]) 
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    # ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)

    # plt.yscale("log")
    # plt.xscale("log")
    # plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/FPGA_PE_scalability_{dataset}_{join_type}_{time}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()


def plot_and_save_raw_performance(dataset, join_type, time='e2e'):

    print(f"===== {dataset}\t{join_type}\t{time}\t =====")
    
    assert time in ["e2e", "kernel"] 
    def get_y_mean_array_with_various_PE_num(max_entry_size, time):
        y_mean_array = []
        for PE_num in PE_nums:
            if time == 'e2e':
                key_sets = [[dataset, join_type, "10000000", "10000000", str(max_entry_size), "mean"]]
            elif time == 'kernel':
                key_sets = [[dataset, join_type, "10000000", "10000000", str(max_entry_size), "kernel_mean"]]
            y_mean = get_array_from_dict(mean_and_error_bar_dict_N_PE[PE_num], key_sets)
            y_mean_array += y_mean
        return y_mean_array
    
    y_mean_8 = get_y_mean_array_with_various_PE_num(8, time)
    y_mean_16 = get_y_mean_array_with_various_PE_num(16, time)
    y_mean_32 = get_y_mean_array_with_various_PE_num(32, time)
    # y_mean_64 = get_y_mean_array_with_various_PE_num(64)
    print(y_mean_8)
    print(y_mean_16)
    print(y_mean_32)
    # print(y_mean_64)

    fig, ax = plt.subplots(1, 1, figsize=(4,2.5))

    label_font = 12
    markersize = 10
    legend_font = 12
    tick_font = 10

    plot_8 = ax.plot(PE_nums, y_mean_8, marker='o', markersize=markersize)
    plot_16 = ax.plot(PE_nums, y_mean_16, marker='^', markersize=markersize)
    plot_32 = ax.plot(PE_nums, y_mean_32, marker='x', markersize=markersize)
    # plot_64 = ax.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
    
    ax.legend([plot_8[0], plot_16[0], plot_32[0]], ["Node size = 8", "Node size = 16", "Node size = 32"], loc='upper right', frameon=False, fontsize=legend_font)
    # ax.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
    # ax.get_xaxis().set_visible(True)
    ax.set_xlabel('Number of join units', fontsize=label_font)
    if time == 'e2e':
        ax.set_ylabel('Latency (end-to-end) in ms', fontsize=label_font)
    elif time == 'kernel':
        ax.set_ylabel('Latency (FPGA kernel only) in ms', fontsize=label_font)

    ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

    ax.set(xlim=[0, 17], ylim=[0, 10000]) 
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    # ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    # ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)

    # plt.yscale("log")
    # plt.xscale("log")
    # plt.rcParams.update({'figure.autolayout': True})

    plt.savefig(f'./images/FPGA_PE_raw_performance_{dataset}_{join_type}_{time}.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()


if __name__ == "__main__":

    datasets = ["Uniform", "OSM"]
    join_types = ["Polygon-Polygon"]
    # join_types = ["Point-in-Polygon", "Polygon-Polygon"]
    times = ["e2e", "kernel"]        
    for dataset in datasets:
        for join_type in join_types:
            for time in times:
                plot_and_save_speedup(dataset, join_type, time)
	

    datasets = ["Uniform", "OSM"]
    # join_types = ["Polygon-Polygon"]
    join_types = ["Point-in-Polygon", "Polygon-Polygon"]
    times = ["e2e"]        
    # times = ["e2e", "kernel"]        
    for dataset in datasets:
        for join_type in join_types:
            for time in times:
                plot_and_save_raw_performance(dataset, join_type, time)