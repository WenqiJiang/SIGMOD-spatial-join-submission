import os
import numpy as np
import argparse

from shapely.geometry import Polygon, box
from typing import Optional


class RandomPointGenerator:
    def __init__(self, distribution: str, map_edge_len: float, obj_edge_len: float, mean: float, stddev: float,
                 scale: float, shift: float, rotation_angle: float, rotation_center_x: float, rotation_center_y: float):
        self.distribution = distribution
        self.range = (0, map_edge_len)
        self.obj_edge_len = obj_edge_len
        self.mean = mean
        self.stddev = stddev
        self.scale = scale
        self.shift = shift
        self.rotation_angle = np.radians(rotation_angle)
        self.rotation_center = np.array([rotation_center_x, rotation_center_y])

    def rotate_points(self, x_array, y_array):
        rotation_matrix = np.array([
            [np.cos(self.rotation_angle), -np.sin(self.rotation_angle)],
            [np.sin(self.rotation_angle), np.cos(self.rotation_angle)]
        ])

        points = np.vstack((x_array, y_array)).T - self.rotation_center
        rotated_points = points @ rotation_matrix.T + self.rotation_center

        return rotated_points[:, 0], rotated_points[:, 1]

    def clip_polygon(self, polygon):
        map_box = box(self.range[0], self.range[0], self.range[1], self.range[1])
        clipped_polygon = polygon.intersection(map_box)

        if clipped_polygon.is_empty:
            return None

        bounds = clipped_polygon.bounds
        return [bounds[0], bounds[2], bounds[1], bounds[3]]

    def generate_polygons(self, num_obj: int, out_file_dir: str, seed: Optional[int] = 123, file_id: Optional[int] = 0, set: Optional[int] = 0):
        np.random.seed(seed)

        if self.distribution == "uniform":
            x_low_array = np.random.rand(num_obj) * self.range[1] + self.range[0]
            x_high_array = x_low_array + self.obj_edge_len

            # seed + 1 for y dimension
            np.random.seed(seed + 1)

            y_low_array = np.random.rand(num_obj) * self.range[1] + self.range[0]
            y_high_array = y_low_array + self.obj_edge_len

        elif self.distribution == "gaussian":
            x_low_array = np.random.normal(self.mean, self.stddev, num_obj)
            x_low_array = np.clip(x_low_array, self.range[0], self.range[1] - self.obj_edge_len)
            x_high_array = x_low_array + self.obj_edge_len

            # seed + 1 for y dimension
            np.random.seed(seed + 1)

            y_low_array = np.random.normal(self.mean, self.stddev, num_obj)
            y_low_array = np.clip(y_low_array, self.range[0], self.range[1] - self.obj_edge_len)
            y_high_array = y_low_array + self.obj_edge_len

        elif self.distribution == "exponential":
            x_low_array = np.random.exponential(self.scale, num_obj) + self.shift
            x_low_array = np.clip(x_low_array, self.range[0], self.range[1] - self.obj_edge_len)
            x_high_array = x_low_array + self.obj_edge_len

            # seed + 1 for y dimension
            np.random.seed(seed + 1)

            y_low_array = np.random.exponential(self.scale, num_obj) + self.shift
            y_low_array = np.clip(y_low_array, self.range[0], self.range[1] - self.obj_edge_len)
            y_high_array = y_low_array + self.obj_edge_len

        else:
            print("Unknown distribution.")
            raise NotImplementedError

        polygons = []
        for i in range(num_obj):
            polygon = Polygon([(x_low_array[i], y_low_array[i]), (x_low_array[i], y_high_array[i]),
                               (x_high_array[i], y_high_array[i]), (x_high_array[i], y_low_array[i])])

            x_coords, y_coords = self.rotate_points(np.array(polygon.exterior.xy[0]), np.array(polygon.exterior.xy[1]))
            rotated_polygon = Polygon(zip(x_coords, y_coords))
            clipped_coords = self.clip_polygon(rotated_polygon)

            if clipped_coords:
                polygons.append(clipped_coords)
            else:
                polygons.append(None)

        file_suffix = f"{self.distribution}_{num_obj}_polygon_file_{file_id}_set_{set}"
        file_path = os.path.join(out_file_dir, self.distribution, "txt", file_suffix + ".txt")
        with open(file_path, "w+") as f_C:
            f_C.write(f"{num_obj}\n")

            for obj_id, coords in enumerate(polygons):
                if coords is None:
                    continue

                f_C.write("{obj_id} {x_low:.2f} {x_high:.2f} {y_low:.2f} {y_high:.2f}\n".format(
                    obj_id=obj_id, x_low=coords[0], x_high=coords[1], y_low=coords[2], y_high=coords[3]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--distribution', type=str, default='uniform', help="uniform, gaussian or exponential")
    parser.add_argument('--num_obj', type=int, default=1000, help="number of objects")
    parser.add_argument('--map_edge_len', type=float, default=10000.0, help="length of the edge of the map (rectangle)")
    parser.add_argument('--obj_edge_len', type=float, default=1.0, help="length of the edge of an object (rectangle)")
    parser.add_argument('--out_file_dir', type=str, default='../data/', help="the output file folder")
    parser.add_argument('--seed', type=int, default=123, help="the seed of random generation")
    parser.add_argument('--file_id', type=int, default=0, help="the file suffix")
    parser.add_argument('--set', type=int, default=0,
                        help="ratio between obj_edge_len and map_edge_len (0 = 1:10000, 1 = 1:1000, 2 = 1:100)")
    parser.add_argument('--mean', type=float, default=5000.0, help="mean of the gaussian distribution")
    parser.add_argument('--stddev', type=float, default=1000.0, help="standard deviation of the gaussian distribution")
    parser.add_argument('--scale', type=float, default=1.0, help="scale param for the exponential distribution")
    parser.add_argument('--shift', type=float, default=0.0, help="shift param for the exponential distribution")

    parser.add_argument('--rotation_angle', type=float, default=0.0, help="rotation angle in degrees")
    parser.add_argument('--rotation_center_x', type=float, default=0.0, help="x-coord of the rotation center")
    parser.add_argument('--rotation_center_y', type=float, default=0.0, help="y-coord of the rotation center")

    args = parser.parse_args()

    rpg = RandomPointGenerator(
        distribution=args.distribution,
        map_edge_len=args.map_edge_len,
        obj_edge_len=args.obj_edge_len,
        mean=args.mean,
        stddev=args.stddev,
        scale=args.scale,
        shift=args.shift,
        rotation_angle=args.rotation_angle,
        rotation_center_x=args.rotation_center_x,
        rotation_center_y=args.rotation_center_y
    )
    rpg.generate_polygons(args.num_obj, args.out_file_dir, args.seed, args.file_id, args.set)

    print("Data generation done!")
