import json
import numpy as np
from scipy import stats


# load results from json file
def load_results(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def calculate_statistics(data):
    data = np.array(data)

    if len(data) == 0:
        return np.nan, np.nan, np.nan, (np.nan, np.nan)

    mean = np.mean(data)
    median = np.median(data)
    std_dev = np.std(data)

    # check if variance is zero (i.e. all values are the same)
    if std_dev == 0:
        # no variability
        conf_interval = (mean, mean)
    else:
        try:
            conf_interval = stats.t.interval(0.95, len(data)-1, loc=mean, scale=stats.sem(data))
        except Exception as e:
            conf_interval = (np.nan, np.nan)
            print(f"Error: Failed to calculate confidence interval! {e}")

    return mean, median, std_dev, conf_interval


def process_results(results, time_keys):
    processed_results = {}

    for description, experiments in results.items():
        for _ in experiments:
            processed_results[description] = {key: [] for key in time_keys}
            processed_results[description]['versions'] = []

            for experiment in experiments:
                version = experiment['version']
                times = experiment['times']
                processed_results[description]['versions'].append(int(version))

                for key in time_keys:
                    time_stats = calculate_statistics(times[key])
                    processed_results[description][key].append(time_stats)

    return processed_results