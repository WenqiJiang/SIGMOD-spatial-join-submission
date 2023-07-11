"""
Show performance scalabiility given different PE num
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import FuncFormatter

from utils import load_json, get_cpp_mean_and_error_bar_dict, get_array_from_dict
plt.style.use('seaborn-colorblind') 

thread_nums = [1, 2, 4, 8, 16]
json_dict_N_PE = dict()
mean_and_error_bar_dict_N_PE = dict()
for thread_num in thread_nums:
    json_dict_N_PE[thread_num] = load_json(f'./json_cpu/CPU_perf_scalability_{thread_num}_threads.json')
    mean_and_error_bar_dict_N_PE[thread_num] = get_cpp_mean_and_error_bar_dict(json_dict_N_PE[thread_num])

if __name__ == "__main__":


    # Use 10M x 10M Uniform Polygon Join
    dataset = "Uniform"
    # dataset = "Uniform"
    join_type = "Polygon-Polygon"
    def get_y_mean_array_with_various_PE_num(max_entry_size):
        y_mean_array = []
        for thread_num in thread_nums:
            key_sets = [ [dataset, join_type, "10000000", "10000000", str(max_entry_size), "mean_bfs_static_ms"]]
            y_mean = get_array_from_dict(mean_and_error_bar_dict_N_PE[thread_num], key_sets)
            y_mean_array += y_mean
        return y_mean_array
    
    y_mean_16 = get_y_mean_array_with_various_PE_num(16)
    y_mean_32 = get_y_mean_array_with_various_PE_num(32)
    y_mean_64 = get_y_mean_array_with_various_PE_num(64)
    print(y_mean_16)
    print(y_mean_32)
    print(y_mean_64)

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
    
    y_speedup_16 = get_speedup_array(y_mean_16)
    y_speedup_32 = get_speedup_array(y_mean_32)
    y_speedup_64 = get_speedup_array(y_mean_64)
    print(y_speedup_16)
    print(y_speedup_32)
    print(y_speedup_64)
    y_linear_speedup = [1, 2, 4, 8, 16]

    fig, ax = plt.subplots(1, 1, figsize=(6, 6))

    label_font = 12
    markersize = 10
    tick_font = 10

    plot0 = ax.plot(thread_nums, y_speedup_16, marker='o', markersize=markersize)
    plot1 = ax.plot(thread_nums, y_speedup_32, marker='^', markersize=markersize)
    plot2 = ax.plot(thread_nums, y_speedup_64, marker='x', markersize=markersize)
	   
    plot3 = ax.plot(thread_nums, y_linear_speedup, linestyle='dashed', color='grey')
    ax.text(8, 8 + 0.5, "Linear speedup", rotation=45)
    
    ax.legend([plot0[0], plot1[0], plot2[0]], ["Max node size = 16", "Max node size = 32", "Max node size = 64"], loc='upper left', frameon=False, fontsize=label_font)
    # ax.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
    # ax.get_xaxis().set_visible(True)
    ax.set_xlabel('Number of join units', fontsize=label_font)
    ax.set_ylabel('Speedup', fontsize=label_font)
    ax.set_title(f'{dataset}, {join_type}, 10M x 10M')

    # ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)

    # plt.yscale("log")
    # plt.xscale("log")
    plt.rcParams.update({'figure.autolayout': True})

    plt.savefig('./images/CPU_threads_scalability.png', transparent=False, dpi=200, bbox_inches="tight")
    # plt.show()
