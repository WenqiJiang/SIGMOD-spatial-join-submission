import os
import numpy as np
import argparse

from typing import Optional

parser = argparse.ArgumentParser()
parser.add_argument('--distribution', type=str, default='uniform', help="uniform or gaussian")
parser.add_argument('--obj_type', type=str, default='polygon', help="polygon or points")
parser.add_argument('--num_obj', type=int, default=1000, help="number of objects")
parser.add_argument('--map_edge_len', type=float, default=10000.0, help="length of the edge of the map (rectangle)")
parser.add_argument('--obj_edge_len', type=float, default=1.0, help="length of the edge of an object (rectangle), only used when obj_type == polygon")
parser.add_argument('--out_file_dir', type=str, default='../generated_data/', help="the output file folder")
parser.add_argument('--seed', type=int, default=123, help="the seed of random generation")
parser.add_argument('--file_id', type=int, default=0, help="the file suffix")

args = parser.parse_args()
distribution = args.distribution
obj_type = args.obj_type
num_obj = args.num_obj
map_edge_len = args.map_edge_len
obj_edge_len = args.obj_edge_len
out_file_dir = args.out_file_dir
seed = args.seed
file_id = args.file_id

class RandomPointGenerator:

    def __init__(self, distribution : str, map_edge_len : float, obj_edge_len : float):
        self.distribution = distribution
        self.range = (0, map_edge_len)
        self.obj_edge_len = obj_edge_len
        #print("map range: {}".format(self.range))
        #print("obj_edge_len: {}".format(obj_edge_len))

    def generate_polygons(self, num_obj : int, out_file_dir : str, seed : Optional[int] = 123, file_id : Optional[int] = 0):

        if self.distribution == "uniform":
            # np.random.rand: Create an array of of random samples from a uniform distribution over [0, 1)
            np.random.seed(seed)
            x_low_array = np.random.rand(num_obj) * self.range[1] + self.range[0]
            x_high_array = x_low_array + self.obj_edge_len

            np.random.seed(seed + 1) # seed + 1 for y dimension
            y_low_array = np.random.rand(num_obj) * self.range[1] + self.range[0]
            y_high_array = y_low_array + self.obj_edge_len

        elif self.distribution == "gaussian":
            print("We need to think carefully about how to implement this as it does not have boundaries.")
            raise NotImplementedError
        
        else:
            print("Unknown distribution.")
            raise NotImplementedError
        file_suffix = f"tile_microbench_map_size_{int(map_edge_len)}_polygon_file_{file_id}"
        with open(os.path.join(out_file_dir, "C_" + file_suffix + ".txt"), "w+") as f_C:
            
            f_C.write(f"{num_obj}\n")

            for obj_id in range(num_obj):
                
                f_C.write("{obj_id} {x_low:.2f} {x_high:.2f} {y_low:.2f} {y_high:.2f}\n".format(
                    obj_id=obj_id, x_low=x_low_array[obj_id], y_low=y_low_array[obj_id], x_high=x_high_array[obj_id], y_high=y_high_array[obj_id]))

    def generate_points(self, num_obj : int, out_file_dir : str, seed : Optional[int] = 123, file_id : Optional[int] = 0):

        if self.distribution == "uniform":
            # np.random.rand: Create an array of of random samples from a uniform distribution over [0, 1)
            np.random.seed(seed)
            x_array = np.random.rand(num_obj) * self.range[1] + self.range[0]

            np.random.seed(seed + 1) # seed + 1 for y dimension
            y_array = np.random.rand(num_obj) * self.range[1] + self.range[0]

        elif self.distribution == "gaussian":
            print("We need to think carefully about how to implement this as it does not have boundaries.")
            raise NotImplementedError
        
        else:
            print("Unknown distribution.")
            raise NotImplementedError

        file_suffix = f"tile_microbench_map_size_{int(map_edge_len)}_point_file_{file_id}"
        with open(os.path.join(out_file_dir, "C_" + file_suffix + ".txt"), "w+") as f_C:
            
            f_C.write(f"{num_obj}\n")

            for obj_id in range(num_obj):
                
                f_C.write("{obj_id} {x_low:.2f} {x_high:.2f} {y_low:.2f} {y_high:.2f}\n".format(
                    obj_id=obj_id, x_low=x_array[obj_id], x_high=x_array[obj_id], y_low=y_array[obj_id], y_high=y_array[obj_id]))
 

if __name__ == "__main__":
    
    rpg = RandomPointGenerator(distribution, map_edge_len, obj_edge_len)
    if obj_type == 'polygon':
        rpg.generate_polygons(num_obj, out_file_dir, seed, file_id)
    elif obj_type == 'point':
        rpg.generate_points(num_obj, out_file_dir, seed, file_id)
    else:
        print("Unknown obj type: ", obj_type)
        raise ValueError

    print("Data gen done!")
