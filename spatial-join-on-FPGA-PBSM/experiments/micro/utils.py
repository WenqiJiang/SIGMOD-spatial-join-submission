import csv
import sys


def handle_error(error, output=None):
    print(f"Error: {error}")
    print()

    if output is not None:
        print("Program output:")
        print()

        for line in output.splitlines():
            print(line)

    sys.exit(1)


def extract_average_time_from_csv(csv_file):
    # open the csv file and locate the relevant section
    with open(csv_file, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            if "Compute Unit Utilization" in row:
                break

        # skip the header and extract 'Average Time (ms)'
        next(reader)
        for row in reader:
            if len(row) > 11:
                return float(row[11])

    handle_error(f"Could not extract average time from {csv_file}.")