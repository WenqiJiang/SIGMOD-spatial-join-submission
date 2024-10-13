from process import load_results
import numpy as np
import matplotlib.pyplot as plt


def visualise_times(data):
    plt.rcParams.update({'font.size': 14})

    time_metric = 'total_time'

    test_descriptions = list(data['dynamic'].keys())

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    axes = axes.flatten()

    for i, test_desc in enumerate(test_descriptions):
        ax = axes[i]

        dynamic_versions = []
        dynamic_times = []
        static_times = []

        for entry in data['dynamic'][test_desc]:
            dynamic_versions.append(entry['version'])
            dynamic_times.append(np.mean(entry['times'][time_metric]))

        for entry in data['static'][test_desc]:
            static_times.append(np.mean(entry['times'][time_metric]))

        # Sort the versions and corresponding times
        sorted_indices = np.argsort(dynamic_versions)
        dynamic_versions = np.array(dynamic_versions)[sorted_indices]
        dynamic_times = np.array(dynamic_times)[sorted_indices]
        static_times = np.array(static_times)[sorted_indices]

        ax.plot(dynamic_versions.astype('str'), dynamic_times, label='Dynamic scheduler', color='#1071e5', marker='o')
        # ax.plot(dynamic_versions.astype('str'), static_times, label='Static scheduler', color='goldenrod', marker='o')
        ax.plot(dynamic_versions.astype('str'), static_times, label='Static scheduler', color='#ff7f0e', marker='o')

        ax.set_title(test_desc)
        ax.set_xlabel('Number of join units (join PEs)')
        ax.set_ylabel('Time (ms)')
        ax.grid(True)
        ax.legend()

    plt.tight_layout()
    plt.savefig("dynamic_vs_static.png", bbox_inches='tight')
    plt.show()


if __name__ == "__main__":
    dynamic_data = load_results("osm.json")
    static_data = load_results("../pbsm_static/osm.json")

    data = {
        "dynamic": dynamic_data,
        "static": static_data
    }

    visualise_times(data)

    dynamic_data = load_results("uniform.json")
    static_data = load_results("../pbsm_static/uniform.json")

    data = {
        "dynamic": dynamic_data,
        "static": static_data
    }

    visualise_times(data)
