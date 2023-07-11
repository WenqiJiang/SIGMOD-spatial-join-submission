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

node_sizes = [4, 8, 16, 32, 48, 64]
node_sizes_txt = [str(i) for i in node_sizes]
# time_sec = np.array([557.766, 585.835, 2014.29, 7614.29, 16871.4, 29785.7]) / 1000
# frequency = 140 * 1e6
time_sec = np.array([391.98, 417.274, 1410, 5330, 11810, 20850]) / 1000
frequency = 200 * 1e6

page_num = 1e6
num_pred_eval = np.array(node_sizes) * np.array(node_sizes) * page_num
cycles = time_sec * frequency
cycles_per_pred = cycles / num_pred_eval
cycles_per_page = cycles / page_num

print("cycles_per_pred: ", cycles_per_pred)

def plot_and_save_cycle_per_pred():
	fig, ax = plt.subplots(1, 1, figsize=(3, 2.5))

	label_font = 11
	markersize = 6
	legend_font = 11
	tick_font = 10

	plot = ax.plot(node_sizes_txt, cycles_per_pred, marker='o', markersize=markersize)
	# plot_64 = ax.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
		
	# ax.text(5, 5 + 1.2, "Linear speedup", rotation=45, fontsize=label_font)

	ax.annotate("Bound by random memory\naccesses for reading nodes", xy=(0, cycles_per_pred[0]), xytext=(0 + 0.6, cycles_per_pred[0] - 1.5), 
	     arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)

	ax.annotate("Close to optimal", xy=(1, cycles_per_pred[1]), xytext=(1 + 0.2, cycles_per_pred[1] + 0.5), 
	     arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)	
	# ax.annotate("Close to optimal", xy=(2, cycles_per_pred[2]), xytext=(2 - 0.2, cycles_per_pred[2] + 0.5), 
	#      arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)

	# ax.legend([plot_8[0], plot_16[0], plot_32[0]], ["Node size = 8", "Node size = 16", "Node size = 32"], loc='upper left', frameon=False, fontsize=legend_font)
	# ax.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
	# ax.get_xaxis().set_visible(True)
	ax.set_xlabel('Node size', fontsize=label_font)
	ax.set_ylabel('Clock cycles per\npredicate evaluation', fontsize=label_font)

	# ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

	plt.hlines(1, 0, 5, color='grey', linestyles='dashed', label='')

	ax.set(ylim=[0, 5]) 
	# ax.xaxis.set_major_locator(MaxNLocator(integer=True))
	# ax.yaxis.set_major_locator(MaxNLocator(integer=True))
	# ax.spines['top'].set_visible(False)
	# ax.spines['right'].set_visible(False)

	# plt.yscale("log")
	# plt.xscale("log")
	# plt.rcParams.update({'figure.autolayout': True})

	plt.savefig(f'./images/join_unit_microbench_cycle_per_pred.png', transparent=False, dpi=200, bbox_inches="tight")
	# plt.show()

def plot_and_save_cycle_per_page():
	fig, ax = plt.subplots(1, 1, figsize=(3, 2.5))

	label_font = 11
	markersize = 6
	legend_font = 10
	tick_font = 10

	plot = ax.plot(node_sizes_txt, cycles_per_page, marker='o', markersize=markersize)
	for i in range(len(node_sizes_txt)):
		ax.text(i, cycles_per_page[i], "{:.1f}".format(cycles_per_page[i]), fontsize=10,
	  	horizontalalignment='center', verticalalignment='bottom')

	# plot_opt = ax.plot(node_sizes_txt, num_pred_eval / page_num, markersize=markersize)
	# plot_64 = ax.plot(PE_nums, y_speedup_64, marker='x', markersize=markersize)
		
	# ax.text(5, 5 + 1.2, "Linear speedup", rotation=45, fontsize=label_font)

	# ax.annotate("Bound by random DRAM accesses\nfor reading node pairs", xy=(0, cycles_per_pred[0]), xytext=(0 + 0.8, cycles_per_pred[0] - 2), 
	#      arrowprops={"arrowstyle": '-|>', 'color': '#2f2f2f', 'linewidth': 1}, fontsize=tick_font)

	# ax.legend([plot[0], plot_opt[0]], ["Achieved performance", "Optimal (1 cycle per predicate eval)"], loc='upper left', frameon=False, fontsize=legend_font)
	
	# ax.tick_params(top=False, bottom=True, left=True, right=False, labelleft=True, labelsize=tick_font)
	# ax.get_xaxis().set_visible(True)
	ax.set_xlabel('Node size', fontsize=label_font)
	ax.set_ylabel('Clock cycles per join\nbetween a pair of nodes', fontsize=label_font)

	# ax.set_title(f'{dataset}, {join_type}, 10M x 10M', fontsize=label_font)

	# plt.hlines(1, 0, 5, color='grey', linestyles='dashed', label='')

	ax.set(xlim = [-0.5, len(node_sizes_txt) - 0.2], ylim=[np.amin(cycles_per_page) / 2, np.amax(cycles_per_page) * 2]) 
	# ax.xaxis.set_major_locator(MaxNLocator(integer=True))
	# ax.yaxis.set_major_locator(MaxNLocator(integer=True))
	# ax.spines['top'].set_visible(False)
	# ax.spines['right'].set_visible(False)

	plt.yscale("log")
	# plt.xscale("log")
	# plt.rcParams.update({'figure.autolayout': True})

	plt.savefig(f'./images/join_unit_microbench_cycle_per_node.png', transparent=False, dpi=200, bbox_inches="tight")
	# plt.show()


if __name__ == '__main__':
	plot_and_save_cycle_per_pred()
	plot_and_save_cycle_per_page()