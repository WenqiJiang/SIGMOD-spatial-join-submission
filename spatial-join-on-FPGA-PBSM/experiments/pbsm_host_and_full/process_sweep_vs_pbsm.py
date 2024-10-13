import json

import matplotlib.pyplot as plt
import numpy as np


def load_data_from_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    return data


def convert_size(size):
    size = int(size)

    if size >= 1_000_000:
        return f"{size // 1_000_000}M"
    elif size >= 1_000:
        return f"{size // 1_000}K"

    return str(size)


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


def plot_experiments(data, time_keys, custom_labels=None, plot_title="Performance Comparison", use_std_err=True):
    plt.rcParams.update({'font.size': 14})

    custom_colors = ['#1071e5', '#008a0e', '#ff7f0e']
    plt.figure(figsize=(10, 6))

    all_handles = []
    all_labels = []

    for i, (folder, runs) in enumerate(data.items()):
        if folder not in time_keys:
            continue

        color = custom_colors[i] if i < len(custom_colors) else None

        for j, time_key in enumerate(time_keys[folder]):
            label = custom_labels.get(f"{folder}_{time_key}",
                                      f"{folder} - {time_key}") if custom_labels else f"{folder} - {time_key}"

            thread_counts = []
            averages = []
            errors = []

            for run in runs:
                thread_counts.append(run['threads'])
                averages.append(run['average'].get(time_key, 0))
                if use_std_err:
                    errors.append(run['std_err'].get(time_key, 0))
                else:
                    errors.append(run['std_dev'].get(time_key, 0))

            thread_counts = np.array(thread_counts)

            # color = custom_colors[i] if j < len(custom_colors) else None

            handle = plt.errorbar(thread_counts.astype('str'), averages, color=color, yerr=errors, fmt='-o',
                                  label=label, capsize=10, capthick=1, zorder=10, ecolor="black")
            all_handles.append(handle)
            all_labels.append(label)

    plt.xlabel('Number of threads')
    plt.ylabel('Time (ms)')
    plt.title(plot_title)
    plt.grid(True)
    plt.legend(all_handles, all_labels, loc='lower center', ncol=3, bbox_to_anchor=(0.5, -0.25))

    plt.savefig("sweep_vs_pbsm", bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    output_file = 'results__OSM_200000_polygon_file_0__OSM_200000_polygon_file_1__5_10.json'

    plot_title = generate_plot_title(output_file)
    data = load_data_from_json(output_file)

    # time keys to plot
    time_keys = {
        'pbsm/parallel': ['cpu_time', 'cpu_refine_time', 'cpu_partition_time', 'cpu_prepare_time'],
        'pbsm/sequential': ['cpu_time'],
        'sweep/parallel': ['cpu_partition_time'],
    }

    # custom labels
    custom_labels = {
        'pbsm/parallel_cpu_time': 'Host partitioning (parallel)',
        'pbsm/sequential_cpu_time': 'Host partitioning (sequential)',
        'sweep/parallel_cpu_partition_time': 'Sweep (parallel)',

        'pbsm/parallel_cpu_partition_time': 'Host: initial partitioning',
        'pbsm/parallel_cpu_refine_time': 'Host: refinement',
        'pbsm/parallel_cpu_prepare_time': 'Host: data preparation',
    }

    custom_colors = ['#1071e5', '#008a0e', '#ff7f0e']
    plt.figure(figsize=(10, 6))

    all_handles = []
    all_labels = []

    # pbsm
    pbsm_runs = data['pbsm/parallel']
    time_key = 'cpu_time'
    label = custom_labels.get(f"{'pbsm/parallel'}_{time_key}")

    thread_counts = []
    averages = []
    errors = []

    for run in pbsm_runs:
        thread_counts.append(run['threads'])
        averages.append(run['average'].get(time_key, 0))
        errors.append(run['std_err'].get(time_key, 0))

    thread_counts = np.array(thread_counts)

    handle = plt.errorbar(thread_counts.astype('str'), averages, color=custom_colors[0], yerr=errors, fmt='-o',
                          label=label, capsize=10, capthick=1, zorder=10, ecolor="black")
    all_handles.append(handle)
    all_labels.append(label)

    # sequential
    seq_runs = data['pbsm/sequential']
    time_key = 'cpu_time'
    label = custom_labels.get(f"{'pbsm/sequential'}_{time_key}")

    thread_counts = []
    averages = []
    errors = []

    for run in seq_runs:
        thread_counts.append(run['threads'])
        averages.append(run['average'].get(time_key, 0))
        errors.append(run['std_err'].get(time_key, 0))

    thread_counts = np.array(thread_counts)

    handle = plt.errorbar(thread_counts.astype('str'), averages, color=custom_colors[1], yerr=errors, fmt='-o',
                          label=label, capsize=10, capthick=1, zorder=10, ecolor="black")
    all_handles.append(handle)
    all_labels.append(label)

    # sweep
    sweep_runs = data['sweep/parallel']
    time_key = 'cpu_partition_time'
    label = custom_labels.get(f"{'sweep/parallel'}_{time_key}")

    thread_counts = []
    averages = []
    errors = []

    for run in sweep_runs:
        thread_counts.append(run['threads'])
        averages.append(run['average'].get(time_key, 0))
        errors.append(run['std_err'].get(time_key, 0))

    thread_counts = np.array(thread_counts)

    handle = plt.errorbar(thread_counts.astype('str'), averages, color=custom_colors[2], yerr=errors, fmt='-o',
                          label=label, capsize=10, capthick=1, zorder=10, ecolor="black")
    all_handles.append(handle)
    all_labels.append(label)

    # details
    pbsm_time_keys = ['cpu_partition_time', 'cpu_refine_time', 'cpu_prepare_time']
    colors = ['violet', 'gray', 'brown']
    for j, time_key in enumerate(pbsm_time_keys):
        label = custom_labels.get(f"{'pbsm/parallel'}_{time_key}")

        thread_counts = []
        averages = []
        errors = []

        for run in pbsm_runs:
            thread_counts.append(run['threads'])
            averages.append(run['average'].get(time_key, 0))
            errors.append(run['std_err'].get(time_key, 0))

        thread_counts = np.array(thread_counts)

        handle = plt.errorbar(thread_counts.astype('str'), averages, color=colors[j], yerr=errors,
                               label=label, capsize=10, capthick=1, zorder=10, ecolor="black",
                              marker='x', linestyle='dashed', alpha=0.8)
        all_handles.append(handle)
        all_labels.append(label)

    plt.xlabel('Number of threads')
    plt.ylabel('Time (ms)')
    plt.title("Partitioning datasets (" + plot_title + ")")
    plt.grid(True)

    reorder = lambda l, nc: sum((l[i::nc] for i in range(nc)), [])

    # plt.legend(all_handles, all_labels, loc='lower center', ncol=3, bbox_to_anchor=(0.5, -0.25))
    plt.legend(reorder(all_handles, 3), reorder(all_labels, 3), loc='lower center', ncol=3, bbox_to_anchor=(0.5, -0.25))
    plt.savefig("sweep_vs_pbsm", bbox_inches='tight')
    plt.show()
