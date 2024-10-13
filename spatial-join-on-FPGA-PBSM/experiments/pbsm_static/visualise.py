import re

from process import load_results, process_results
import numpy as np
import matplotlib.pyplot as plt


def parse_size_from_description(description):
    # extract the size part from the description
    match = re.search(r'(\d+[KM]) x (\d+[KM])', description)

    if match:
        first_size = match.group(1)
        second_size = match.group(2)

        first_multiplier = 1000 if 'K' in first_size else 1000000
        second_multiplier = 1000 if 'K' in second_size else 1000000

        first_size = int(first_size.replace('K', '').replace('M', '')) * first_multiplier
        second_size = int(second_size.replace('K', '').replace('M', '')) * second_multiplier

        return (first_size, second_size), match.group(0)

    return (float('inf'), float('inf')), description


def visualise_times(processed_results, time_keys, labels):
    plt.rcParams.update({'font.size': 14})

    custom_colors = ['#1071e5', '#008a0e', '#ff7f0e']
    # custom_colors = ['#008a0e', '#ff7f0e', '#1071e5']

    # extract and sort sizes based on parsed numeric values
    size_pairs = []
    for distribution_name, dist_results in processed_results.items():
        for description in dist_results.keys():
            size_pair, size_str = parse_size_from_description(description)
            size_pairs.append((size_pair, size_str))

    size_pairs = sorted(set(size_pairs))

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    axes = axes.flatten()

    all_handles = []
    all_labels = []

    for i, (size_pair, size_str) in enumerate(size_pairs):
        ax = axes[i]

        for j, (distribution_name, dist_results) in enumerate(processed_results.items()):
            # find the correct description key that matches the current size
            matching_description = None
            for description in dist_results.keys():
                if size_str in description:
                    matching_description = description
                    break

            if not matching_description:
                print(f"Warning: No matching description found for size '{size_str}' in distribution '{distribution_name}'. Skipping...")
                continue

            time_stats = dist_results[matching_description]
            versions = np.array(time_stats['versions'])

            color = custom_colors[j] if j < len(custom_colors) else None

            for k, key in enumerate(time_keys):
                time_means = np.array([s[0] for s in time_stats[key]])
                time_err_intervals = np.array([s[3] for s in time_stats[key]])
                time_errs = np.array([[mean - ci[0], ci[1] - mean] for mean, ci in zip(time_means, time_err_intervals)]).T

                sorted_indices = np.argsort(versions)
                versions_sorted = versions[sorted_indices]
                time_means_sorted = time_means[sorted_indices]
                time_errs_sorted = time_errs[:, sorted_indices]

                # color = custom_colors[k] if k < len(custom_colors) else None

                handle = ax.errorbar(versions_sorted.astype('str'), time_means_sorted, yerr=time_errs_sorted, fmt='-o', capsize=10,
                                     capthick=1, label=f'{distribution_name} {labels[key]}', zorder=10, ecolor="black", color=color)

                # only add handles and labels once per distribution
                # i.e. collect labels for first plot only
                if i == 0 and k == 0:
                    all_handles.append(handle)
                    all_labels.append(f'{distribution_name}')

        ax.set_title(f'{size_str}')
        if i > 1:
            ax.set_xlabel('Number of join units (join PEs)')
        if i % 2 == 0:
            ax.set_ylabel('Time (ms)')
        # ax.legend(loc="upper right")
        ax.grid(axis='y')

    fig.legend(all_handles, all_labels, loc='lower center', ncol=len(all_labels), bbox_to_anchor=(0.5, -0.05))

    # plt.tight_layout()
    plt.savefig("PBSM_FPGA_overall", bbox_inches='tight')
    plt.show()


if __name__ == "__main__":
    # load results from json file
    uniform = load_results('uniform.json')
    gaussian = load_results('gaussian.json')
    osm = load_results('osm.json')

    time_keys = [
        'total_time',
        # 'cpu_time'
    ]

    labels = {
        'total_time': "Total time",
        # 'cpu_time': "Host time"
    }

    # process results to get stats
    uniform = process_results(uniform, time_keys)
    gaussian = process_results(gaussian, time_keys)
    osm = process_results(osm, time_keys)

    all_results = {
        "Uniform": uniform,
        "Gaussian": gaussian,
        "OSM": osm
    }

    visualise_times(all_results, time_keys, labels)
