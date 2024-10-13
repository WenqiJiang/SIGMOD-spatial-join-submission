import argparse
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np


def parse_file(filename):
    with open(filename, 'r') as file:
        num_obj = int(file.readline().strip())
        objects = []

        for line in file:
            parts = line.split()
            obj_id = int(parts[0])
            low0 = float(parts[1])
            high0 = float(parts[2])
            low1 = float(parts[3])
            high1 = float(parts[4])

            center_x = (low0 + high0) / 2
            center_y = (low1 + high1) / 2

            objects.append((obj_id, low0, high0, low1, high1, center_x, center_y))

    return num_obj, objects


def convert_size(size):
    size = int(size)

    if size >= 1_000_000:
        return f"{size // 1_000_000}M"
    elif size >= 1_000:
        return f"{size // 1_000}K"

    return str(size)


def generate_plot_title(file_name):
    parts = file_name.split('/')
    name = parts[-1].split("_")

    distribution = name[0].capitalize() if name[0].islower() else name[0]
    size = convert_size(name[1])

    return f"{distribution}, {size}"


def visualise_markers(objects, title, marker_size=5):
    plt.figure(figsize=(10, 10))

    for obj in objects:
        obj_id, low0, high0, low1, high1, center_x, center_y = obj
        plt.scatter(center_x, center_y, c='#1071e5', s=marker_size, marker='o')

    # auto adjust limits based on data
    all_x = [center_x for _, _, _, _, _, center_x, _ in objects]
    all_y = [center_y for _, _, _, _, _, _, center_y in objects]

    plt.xlim(min(all_x) - 1, max(all_x) + 1)
    plt.ylim(min(all_y) - 1, max(all_y) + 1)

    ax = plt.gca()
    plt.xlabel('x')
    plt.ylabel('y')
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    plt.grid(True)
    plt.title(title)

    plt.savefig(title, bbox_inches='tight')
    plt.show()


def visualise_polygons(objects, title):
    plt.figure(figsize=(10, 10))

    for obj in objects:
        obj_id, low0, high0, low1, high1, center_x, center_y = obj
        rect = patches.Rectangle((low0, low1), high0 - low0, high1 - low1, edgecolor='#1071e5', facecolor='none', linewidth=1)
        plt.gca().add_patch(rect)

    # auto adjust limits based on data
    all_lows = [min(low0, low1) for _, low0, high0, low1, high1, _, _ in objects]
    all_highs = [max(high0, high1) for _, low0, high0, low1, high1, _, _ in objects]

    plt.xlim(min(all_lows) - 1, max(all_highs) + 1)
    plt.ylim(min(all_lows) - 1, max(all_highs) + 1)

    ax = plt.gca()
    plt.xlabel('x')
    plt.ylabel('y')
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    plt.grid(True)
    plt.title(title)

    plt.savefig(title, bbox_inches='tight')
    plt.show()


def generate_heatmap(objects, title, bins=100):
    x_array = []
    y_array = []

    for obj in objects:
        obj_id, low0, high0, low1, high1, _, _ = obj
        x_array.extend([low0, high0])
        y_array.extend([low1, high1])

    H, xedges, yedges = np.histogram2d(
        x_array, y_array,
        bins=bins,
        range=[[min(x_array), max(x_array)], [min(y_array), max(y_array)]]
    )

    extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]

    plt.figure(figsize=(10, 10))
    plt.imshow(H.T, interpolation='nearest', origin='lower', cmap='hot', extent=extent)
    plt.colorbar()
    plt.title(title)
    ax = plt.gca()
    plt.xlabel('x')
    plt.ylabel('y')
    ax.set_xticklabels([])
    ax.set_yticklabels([])

    plt.savefig(title, bbox_inches='tight')
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualise objects from a data file.")
    parser.add_argument("filename", help="Path to the data file")
    parser.add_argument("--heatmap", action="store_true", help="Generate a heatmap instead of rectangles")
    parser.add_argument("--markers", action="store_true", help="Visualise center points instead of rectangles")
    parser.add_argument("--marker_size", type=int, default=5, help="Size of the markers (dots) to plot")
    args = parser.parse_args()

    num_obj, objects = parse_file(args.filename)

    title = generate_plot_title(args.filename)

    plt.rcParams.update({'font.size': 14})

    if args.heatmap:
        generate_heatmap(objects, title)
    elif args.markers:
        visualise_markers(objects, title, marker_size=args.marker_size)
    else:
        visualise_polygons(objects, title)
