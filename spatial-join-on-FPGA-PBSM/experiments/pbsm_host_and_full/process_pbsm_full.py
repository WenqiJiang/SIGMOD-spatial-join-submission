import json
from process import process_results
import matplotlib.pyplot as plt
import numpy as np


def load_data_from_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    return data


def generate_plot_title(json_name):
    parts = json_name.split('__')
    file_name_a = parts[1]
    file_name_b = parts[2]

    def extract_info(file_name):
        parts = file_name.split('_')
        distribution = parts[0].capitalize() if parts[0].islower() else parts[0]
        size = convert_size(parts[1])
        return distribution, size

    dist_a, size_a = extract_info(file_name_a)
    _, size_b = extract_info(file_name_b)

    return f"{dist_a}, {size_a} x {size_b}"


def convert_size(size):
    size = int(size)

    if size >= 1_000_000:
        return f"{size // 1_000_000}M"
    elif size >= 1_000:
        return f"{size // 1_000}K"

    return str(size)


def compute_stats(times):
    avg = np.mean(times)
    std_dev = np.std(times)
    std_err = std_dev / np.sqrt(len(times))
    return avg, std_dev, std_err


def plot_experiment(experiment, host_results, fpga_results, output_filename):
    plt.rcParams.update({'font.size': 14})
    plt.figure(figsize=(10, 6))

    time_keys = [
        'total_time',
        'cpu_time'
    ]

    all_handles = []
    all_labels = []

    plot_title = generate_plot_title(host_results)
    host_data = load_data_from_json(host_results)
    # host_pbsm_data = host_data["pbsm/parallel"]

    fpga_data = load_data_from_json(fpga_results)
    fpga_data = process_results(fpga_data, time_keys)
    fpga_data = fpga_data[experiment]

    threads = np.array(fpga_data['versions'])

    custom_colors = ['#1071e5', '#008a0e', '#ff7f0e']

    # HOST PART
    pbsm_runs = host_data['pbsm/parallel']
    time_key = 'cpu_time'
    label = 'Host code (partitioning)'

    thread_counts = []
    averages = []
    errors = []

    for run in pbsm_runs:
        # if run['threads'] == 16:
        #     continue

        thread_counts.append(run['threads'])
        averages.append(run['average'].get(time_key, 0))
        errors.append(run['std_err'].get(time_key, 0))

    thread_counts = np.array(thread_counts)

    handle = plt.errorbar(thread_counts.astype('str'), averages, color='violet', yerr=errors, fmt='-o',
                          label=label, capsize=10, capthick=1, zorder=10, ecolor="black", marker='x',
                          linestyle='dashed', alpha=0.8)
    all_handles.append(handle)
    all_labels.append(label)

    # FPGA part
    key = "total_time"
    time_means = np.array([s[0] for s in fpga_data[key]])
    time_err_intervals = np.array([s[3] for s in fpga_data[key]])
    time_errs = np.array([[mean - ci[0], ci[1] - mean] for mean, ci in zip(time_means, time_err_intervals)]).T

    sorted_indices = np.argsort(threads)
    versions_sorted = threads[sorted_indices]
    time_means_sorted = time_means[sorted_indices]
    time_errs_sorted = time_errs[:, sorted_indices]

    handle = plt.errorbar(versions_sorted.astype('str'), time_means_sorted, yerr=time_errs_sorted, fmt='-o', capsize=10,
                          capthick=1, label=f'FPGA code (join)', zorder=10, ecolor="black", color='brown', marker='x',
                          linestyle='dashed', alpha=0.8)

    # sum of Host and FPGA average times
    combined_averages = np.array(averages) + time_means_sorted
    combined_errors = np.sqrt(np.array(errors) ** 2 + time_errs_sorted[0] ** 2)

    handle = plt.errorbar(versions_sorted.astype('str'), combined_averages, yerr=combined_errors, fmt='-o', capsize=10,
                          capthick=1, label='Combined Host + FPGA', zorder=10, ecolor="black", color=custom_colors[0],
                          marker='o', linestyle='solid', alpha=0.8)
    all_handles.append(handle)
    all_labels.append('Host & FPGA code')

    # SWEEP
    sweep_runs = host_data['sweep/parallel']
    time_key = 'cpu_time'
    label = "Plane Sweep (partition and join)"

    thread_counts = []
    averages = []
    errors = []

    for run in sweep_runs:
        # if run['threads'] == 16:
        #     continue

        thread_counts.append(run['threads'])
        averages.append(run['average'].get(time_key, 0))
        errors.append(run['std_err'].get(time_key, 0))

    thread_counts = np.array(thread_counts)

    handle = plt.errorbar(thread_counts.astype('str'), averages, yerr=errors, fmt='-o',
                          label=label, capsize=10, capthick=1, zorder=10, ecolor="black", color=custom_colors[2])
    all_handles.append(handle)
    all_labels.append(label)

    plt.xlabel('Number of threads (join PEs)')
    plt.ylabel('Time (ms)')
    plt.title(plot_title)
    plt.grid(True)
    plt.legend()

    plt.savefig(output_filename, bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    plot_experiment(
        experiment="OSM, 200K x 200K",
        host_results='results__OSM_200000_polygon_file_0__OSM_200000_polygon_file_1__5_10.json',
        fpga_results='../pbsm_static/osm.json',
        output_filename="PBSM_full_osm.png"
    )

    plot_experiment(
        experiment="Uniform, 200K x 200K",
        host_results='results__uniform_200000_polygon_file_0_set_0__uniform_200000_polygon_file_1_set_0__5_10.json',
        fpga_results='../pbsm_static/uniform.json',
        output_filename="PBSM_full_uniform.png"
    )