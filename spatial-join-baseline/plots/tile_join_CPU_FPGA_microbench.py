"""
Compare nested loop join (CPU, FPGA) and plane sweep join (CPU) performance using a single core / PE

"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import FuncFormatter

from utils import load_FPGA_json, get_FPGA_mean_and_error_bar_dict, get_array_from_dict
# plt.style.use('seaborn-colorblind') 
# plt.style.use('ggplot') 
plt.style.use('seaborn-pastel')
# plt.style.use('seaborn')
# plt.style.use('fivethirtyeight')

def get_default_colors():

  default_colors = []
  for i, color in enumerate(plt.rcParams['axes.prop_cycle']):
      default_colors.append(color["color"])
      print(color["color"], type(color["color"]))

  return default_colors

default_colors = get_default_colors()

# === FPGA nested loop join === #

# 140 MHz perf, 1 M pages
# page size = 2 -> 557.259 ms
# page size = 4 -> 557.766 ms
# page size = 8 -> 585.835 ms
# page size = 16 -> 2014.29 ms
# page size = 32 -> 7614.29 ms
# page size = 48 -> 16871.4 ms
# page size = 64 -> 29785.7 ms

# 200 MHz perf, 1 M pages
# page size = 2 -> 391.199 ms
# page size = 4 -> 391.98 ms
# page size = 8 -> 417.274 ms
# page size = 16 -> 1410 ms
# page size = 32 -> 5330 ms
# page size = 48 -> 11810 ms
# page size = 64 -> 20850 ms

# CPU : less populated tiles
# ===== Datasets: C_tile_microbench_map_size_10000_polygon_file_0.txt, C_tile_microbench_map_size_10000_polygon_file_1.txt =====

# Tile size:  4
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 4 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 0.33 us      (Plane Sweep):  3.21 us

# Tile size:  8
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 8 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 1.24 us      (Plane Sweep):  6.60 us

# Tile size:  16
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 16 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 5.00 us      (Plane Sweep):  13.53 us

# Tile size:  32
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 32 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 19.97 us     (Plane Sweep):  28.23 us

# Tile size:  64
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 64 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 79.91 us     (Plane Sweep):  58.03 us

# Tile size:  128
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_10000_polygon_file_1.txt 128 > cpu_microbench_log
#         Num results (Nested Loop): 0    (Plane Sweep): 0
#         Per tile time in us (Nested Loop): 323.38 us    (Plane Sweep):  122.43 us

# CPU : highly populated tiles
# ===== Datasets: C_tile_microbench_map_size_5_polygon_file_0.txt, C_tile_microbench_map_size_5_polygon_file_1.txt =====

# Tile size:  4
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 4 > cpu_microbench_log
#         Num results (Nested Loop): 1    (Plane Sweep): 1
#         Per tile time in us (Nested Loop): 0.43 us      (Plane Sweep):  3.60 us

# Tile size:  8
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 8 > cpu_microbench_log
#         Num results (Nested Loop): 9    (Plane Sweep): 9
#         Per tile time in us (Nested Loop): 1.92 us      (Plane Sweep):  8.54 us

# Tile size:  16
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 16 > cpu_microbench_log
#         Num results (Nested Loop): 30   (Plane Sweep): 30
#         Per tile time in us (Nested Loop): 7.36 us      (Plane Sweep):  19.42 us

# Tile size:  32
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 32 > cpu_microbench_log
#         Num results (Nested Loop): 135  (Plane Sweep): 135
#         Per tile time in us (Nested Loop): 29.36 us     (Plane Sweep):  49.15 us

# Tile size:  64
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 64 > cpu_microbench_log
#         Num results (Nested Loop): 550  (Plane Sweep): 550
#         Per tile time in us (Nested Loop): 118.54 us    (Plane Sweep):  142.30 us

# Tile size:  128
# Executing:  /mnt/scratch/wenqi/spatial-join-baseline/cpp/tile_join_microbench /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_0.txt /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_tile_microbench_map_size_5_polygon_file_1.txt 128 > cpu_microbench_log
#         Num results (Nested Loop): 2170 (Plane Sweep): 2170
#         Per tile time in us (Nested Loop): 492.94 us    (Plane Sweep):  410.77 us

node_sizes = [4, 8, 16, 32, 64, 128]
node_sizes_txt = [str(i) for i in node_sizes]
# time_sec = np.array([557.766, 585.835, 2014.29, 7614.29, 16871.4, 29785.7]) / 1000
# frequency = 140 * 1e6
page_num = 1e6
FPGA_time_us = (np.array([391.98, 417.274, 1410, 5330, 20850, 20850 * 4]) / 1000 / page_num) * 1e6
CPU_nested_loop_low_results_us = np.array([0.33, 1.24, 5.00, 19.97, 79.91, 323.38])
CPU_nested_loop_high_results_us = np.array([0.43, 1.92, 7.36, 29.36, 118.54, 492.94])
CPU_plane_sweep_low_results_us = np.array([3.21, 6.60, 13.53, 28.23, 58.03, 122.43])
CPU_plane_sweep_high_results_us = np.array([3.60, 8.54, 19.42, 49.15, 142.30, 410.77])

# pop the first elements
node_sizes_txt = node_sizes_txt[1:]
FPGA_time_us = FPGA_time_us[1:]
CPU_nested_loop_low_results_us = CPU_nested_loop_low_results_us[1:]
CPU_nested_loop_high_results_us = CPU_nested_loop_high_results_us[1:]
CPU_plane_sweep_low_results_us = CPU_plane_sweep_low_results_us[1:]
CPU_plane_sweep_high_results_us = CPU_plane_sweep_high_results_us[1:]

print("Node sizes: ", node_sizes_txt)
print("Time consumtion High / Low cardinality, CPU nested loop: ", CPU_nested_loop_high_results_us / CPU_nested_loop_low_results_us)
print("Time consumtion High / Low cardinality, CPU plane sweep: ", CPU_plane_sweep_high_results_us / CPU_plane_sweep_low_results_us)

print("low cardinality speedup: Plane sweep vs nested loop CPU: ", CPU_nested_loop_low_results_us / CPU_plane_sweep_low_results_us)
print("high cardinality speedup: Plane sweep vs nested loop CPU: ", CPU_nested_loop_high_results_us / CPU_plane_sweep_high_results_us)

print("FPGA speedup low cardinality / CPU nested loop low cardinality: ", CPU_nested_loop_low_results_us / FPGA_time_us)
print("FPGA speedup high cardinality / CPU nested loop high cardinality: ", CPU_nested_loop_high_results_us / FPGA_time_us)
print("FPGA speedup low cardinality / CPU plane sweep low cardinality: ", CPU_plane_sweep_low_results_us / FPGA_time_us)
print("FPGA speedup high cardinality / CPU plane sweep high cardinality: ", CPU_plane_sweep_high_results_us / FPGA_time_us)


def plot_and_save_one_subplot():

	fig, ax0 = plt.subplots(1, 1, figsize=(5, 1.5))

	label_font = 11
	markersize = 6
	legend_font = 9
	tick_font = 9
	text_font = 9

	plot_fpga_low_card = ax0.plot(node_sizes_txt, FPGA_time_us, marker='o', markersize=markersize)#, color=default_colors[0])
	plot_nested_loop_low_card = ax0.plot(node_sizes_txt, CPU_nested_loop_low_results_us, marker='X', markersize=markersize)# , color=default_colors[0])
	plot_plane_sweep_low_card = ax0.plot(node_sizes_txt, CPU_plane_sweep_low_results_us, marker='^', markersize=markersize)# , color=default_colors[0])

	plot_fpga_high_card = ax0.plot(node_sizes_txt, FPGA_time_us, marker='o', markersize=markersize)# , color=default_colors[2])
	plot_nested_loop_high_card = ax0.plot(node_sizes_txt, CPU_nested_loop_high_results_us, marker='X', markersize=markersize , color='#b0b0b0')
	plot_plane_sweep_high_card = ax0.plot(node_sizes_txt, CPU_plane_sweep_high_results_us, marker='^', markersize=markersize , color=default_colors[5])

	# plot_opt = ax0.plot(node_sizes_txt, num_pred_eval / page_num, markersize=markersize)
	# plot_64 = ax0.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
		

	# txt = "the performance of the SwiftSpatial join unit is irrelevant\nto the result cardinality (overlapped lines)"
	txt = "SwiftSpatial NL performance\nlow card. & high card. (overlapped)"
	ax0.annotate(txt, xy=(1, FPGA_time_us[1]), xytext=(1.6, FPGA_time_us[0]), 
	     arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)

	ax0.legend([plot_fpga_low_card[0], plot_nested_loop_low_card[0], plot_plane_sweep_low_card[0], \
			 		   plot_fpga_high_card[0], plot_nested_loop_high_card[0], plot_plane_sweep_high_card[0]],
		   ["SwiftSpatial NL (low card.)", "CPU NL (low card.)", "CPU PS (low card.)", "SwiftSpatial NL (high card.)", "CPU NL (high card.)", "CPU PS (high card.)"], loc=(-0.14, 1.04), ncol=2, frameon=False, fontsize=legend_font)
	
	ax0.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
	# ax0.get_xax0is().set_visible(True)
	ax0.set_xlabel('Tile Size', fontsize=label_font)
	ax0.set_ylabel('Latency (us)', fontsize=label_font)

	# ax1.set_ylabel('Latency (us)', fontsize=label_font)

	# ax0.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

	# plt.hlines(1, 0, 5, color='grey', linestyles='dashed', label='')

	# ax0.set(xlim = [-0.5, len(node_sizes_txt) - 0.2], ylim=[np.amin(cycles_per_page) / 2, np.amax0(cycles_per_page) * 2]) 
	# ax0.xax0is.set_major_locator(Max0NLocator(integer=True))
	# ax0.yax0is.set_major_locator(Max0NLocator(integer=True))
	# ax0.spines['top'].set_visible(False)
	# ax0.spines['right'].set_visible(False)

	ax0.set_yscale("log")
	# ax0.set_ylim([0.1, 1000])

	# plt.xscale("log")
	# plt.rcParams.update({'figure.autolayout': True})

	plt.savefig(f'./images/tile_join_microbench_plane_sweep_nested_loop.png', transparent=False, dpi=200, bbox_inches="tight")
	# plt.show()


def plot_and_save_two_subplots():

	fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(6, 2.5))

	label_font = 11
	markersize = 6
	legend_font = 9
	tick_font = 9
	text_font = 9

	plot_fpga_low_card = ax0.plot(node_sizes_txt, FPGA_time_us, marker='o', markersize=markersize)
	plot_nested_loop_low_card = ax0.plot(node_sizes_txt, CPU_nested_loop_low_results_us, marker='X', markersize=markersize)
	plot_plane_sweep_low_card = ax0.plot(node_sizes_txt, CPU_plane_sweep_low_results_us, marker='^', markersize=markersize)

	plot_fpga_high_card = ax1.plot(node_sizes_txt, FPGA_time_us, marker='o', markersize=markersize)
	plot_nested_loop_high_card = ax1.plot(node_sizes_txt, CPU_nested_loop_high_results_us, marker='X', markersize=markersize)
	plot_plane_sweep_high_card = ax1.plot(node_sizes_txt, CPU_plane_sweep_high_results_us, marker='^', markersize=markersize)	

	# plot_opt = ax0.plot(node_sizes_txt, num_pred_eval / page_num, markersize=markersize)
	# plot_64 = ax0.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
		
	ax0.text(1, 0.2, "Low result cardinality", rotation=0, fontsize=text_font)
	ax1.text(1, 0.2, "High result cardinality", rotation=0, fontsize=text_font)

	# ax0.annotate("Bound by random DRAM accesses\nfor reading node pairs", xy=(0, cycles_per_pred[0]), xytext=(0 + 0.8, cycles_per_pred[0] - 2), 
	#      arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)

	ax0.legend([plot_fpga_low_card[0], plot_nested_loop_low_card[0], plot_plane_sweep_low_card[0]], 
		   ["SwiftSpatial nested loop", "CPU nested loop", "CPU plane sweep"], loc=(-0.27, 1.04), ncol=3, frameon=False, fontsize=legend_font)
	
	ax0.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
	ax1.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
	# ax0.get_xax0is().set_visible(True)
	ax0.set_xlabel('Tile Size', fontsize=label_font)
	ax0.set_ylabel('Latency (us)', fontsize=label_font)

	ax1.set_xlabel('Tile Size', fontsize=label_font)
	# ax1.set_ylabel('Latency (us)', fontsize=label_font)

	# ax0.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

	# plt.hlines(1, 0, 5, color='grey', linestyles='dashed', label='')

	# ax0.set(xlim = [-0.5, len(node_sizes_txt) - 0.2], ylim=[np.amin(cycles_per_page) / 2, np.amax0(cycles_per_page) * 2]) 
	# ax0.xax0is.set_major_locator(Max0NLocator(integer=True))
	# ax0.yax0is.set_major_locator(Max0NLocator(integer=True))
	# ax0.spines['top'].set_visible(False)
	# ax0.spines['right'].set_visible(False)

	ax0.set_yscale("log")
	ax1.set_yscale("log")
	ax0.set_ylim([0.1, 1000])
	ax1.set_ylim([0.1, 1000])

	# plt.xscale("log")
	# plt.rcParams.update({'figure.autolayout': True})

	plt.savefig(f'./images/tile_join_microbench_plane_sweep_nested_loop.png', transparent=False, dpi=200, bbox_inches="tight")
	# plt.show()


if __name__ == '__main__':
	plot_and_save_one_subplot()