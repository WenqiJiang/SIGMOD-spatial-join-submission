# Data Generation Scripts

This folder contains scripts for generating uniform, exponential and Gaussian datasets, converting them from text to binary format and visualizing the generated datasets. 

## Scripts Overview

### 1. Object Visualisation Script

This script visualises objects from a data file with multiple modes, including polygons (default), heatmaps or center points (markers).

#### Usage

```
python visualise_objects.py <<<filename>>> [--heatmap] [--markers] [--marker_size <<<SIZE>>>]
```

- `<<<filename>>>`: Path to the data file to be visualised.
- `--heatmap`: (Optional) Generate a heatmap instead of polygons.
- `--markers`: (Optional) Visualize center points (markers) instead of polygons.
- `--marker_size <<<SIZE>>>`: (Optional) Set the size of the markers (default: 5).

#### Example

```
python visualise_objects.py data/objects_file.bin --markers --marker_size 10
```

This will visualise the center points of objects with a marker size of 10.

### 2. Dataset Generation Script

This script generates datasets using uniform, Gaussian or exponential distributions and saves them to a text file.

#### Usage

```
python generate_datasets.py --distribution <<<distribution_type>>> --num_obj <<<num>>> --map_edge_len <<<length>>> --out_file_dir <<<output_dir>>> [options]
```

- `--distribution`: Distribution type to use for generating the dataset. Can be `uniform`, `gaussian`, or `exponential`. (default: `uniform`)
- `--num_obj`: Number of objects to generate. (default: 1000)
- `--map_edge_len`: Length of the edge of the map (rectangle). (default: 10000.0)
- `--obj_edge_len`: Length of the edge of each object (rectangle). (default: 1.0)
- `--out_file_dir`: Directory where the output file will be saved. (default: `../data/`)
- `--seed`: Seed for random number generation. (default: 123)
- `--file_id`: Suffix for the output file. (default: 0)
- `--mean`: Mean for the Gaussian distribution. (default: 5000.0)
- `--stddev`: Standard deviation for the Gaussian distribution. (default: 1000.0)
- `--scale`: Scale parameter for the exponential distribution. (default: 1.0)
- `--shift`: Shift parameter for the exponential distribution. (default: 0.0)
- `--rotation_angle`: Rotation angle for the generated map objects in degrees. (default: 0.0)
- `--rotation_center_x`: X-coordinate of the rotation center. (default: 0.0)
- `--rotation_center_y`: Y-coordinate of the rotation center. (default: 0.0)

#### Example

```
python data_gen.py --distribution gaussian --num_obj 500 --map_edge_len 10000 --obj_edge_len 2.0 --out_file_dir ../data/ --seed 42 --mean 6000 --stddev 800 --rotation_angle 45 --file_id 1
```

This will generate 500 objects using a Gaussian distribution and save them to the specified directory.


### 3. Text to Binary File Conversion Script

This script converts txt files containing dataset information to bin format.

#### Usage

```
python txt_to_bin.py --txt_files <<<txt_file_1>>> <<<txt_file_2>>> ... --bin_files <<<bin_file_1>>> <<<bin_file_2>>> ...
```

- `--txt_files`: List of paths to txt files.
- `--bin_files`: List of corresponding bin file paths. Must be the same number of bin files as txt files.

#### Example

```
python txt_to_bin.py --txt_files ../data/gaussian/txt/gaussian_50000_polygon_file_0_set_0.txt ../data/gaussian/txt/gaussian_200000_polygon_file_0_set_0.txt --bin_files ../data/gaussian/bin/gaussian_50000_polygon_file_0_set_0.bin ../data/gaussian/bin/gaussian_200000_polygon_file_0_set_0.bin
```