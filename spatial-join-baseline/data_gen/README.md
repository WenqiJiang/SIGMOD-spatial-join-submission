# Data Generation

We provide two main scripts for data generation, for OpenStreetMap (OSM) and synthesized datasets, respectively.

## Synthesized dataset

data_gen.py

Commands we use for generating files of different sizes (We always use 123 as the seed for file 0 and 456 for file 1): 

Uniform distribution polygons:

```
# 1K
python data_gen.py --distribution uniform --obj_type polygon --num_obj 1000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 123 --file_id 0
python data_gen.py --distribution uniform --obj_type polygon --num_obj 1000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 456 --file_id 1
# 10K
python data_gen.py --distribution uniform --obj_type polygon --num_obj 10000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 123 --file_id 0
python data_gen.py --distribution uniform --obj_type polygon --num_obj 10000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 456 --file_id 1
# 100K
python data_gen.py --distribution uniform --obj_type polygon --num_obj 100000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 123 --file_id 0
python data_gen.py --distribution uniform --obj_type polygon --num_obj 100000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 456 --file_id 1
# 1M
python data_gen.py --distribution uniform --obj_type polygon --num_obj 1000000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 123 --file_id 0
python data_gen.py --distribution uniform --obj_type polygon --num_obj 1000000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 456 --file_id 1
# 10M
python data_gen.py --distribution uniform --obj_type polygon --num_obj 10000000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 123 --file_id 0
python data_gen.py --distribution uniform --obj_type polygon --num_obj 10000000 --map_edge_len 10000.0 --obj_edge_len 1.0 --out_file_dir ../generated_data --seed 456 --file_id 1
```

Uniform distribution points:

```
# 1K
python data_gen.py --distribution uniform --obj_type point --num_obj 1000 --map_edge_len 10000.0 --out_file_dir ../generated_data --seed 789 --file_id 0 
# 10K
python data_gen.py --distribution uniform --obj_type point --num_obj 10000 --map_edge_len 10000.0 --out_file_dir ../generated_data --seed 789 --file_id 0 
# 100K
python data_gen.py --distribution uniform --obj_type point --num_obj 100000 --map_edge_len 10000.0 --out_file_dir ../generated_data --seed 789 --file_id 0 
# 1M
python data_gen.py --distribution uniform --obj_type point --num_obj 1000000 --map_edge_len 10000.0 --out_file_dir ../generated_data --seed 789 --file_id 0 
# 10M
python data_gen.py --distribution uniform --obj_type point --num_obj 10000000 --map_edge_len 10000.0 --out_file_dir ../generated_data --seed 789 --file_id 0 
```

## OpenStreetMap

We follow the VLDB'18 paper "How Good Are Modern Spatial Analytics Systems?", and use the OSM building subset, which contains 115M polygons. We then turn the polygons into rectangles. 

The OpenStreetMap building dataset (115M) released by SpatialHadoop: http://spatialhadoop.cs.umn.edu/datasets.html

To download from google drive using commands:

```
pip install gdown
# building dataset
gdown https://drive.google.com/u/0/uc?id=0B1jY75xGiy7ecW0tTFJSczdkSzQ&export=download&resourcekey=0-Or_UPhT-2MBlZmE5X5pcsg
bzip2 -d buildings.bz2
# nodes dataset
gdown https://drive.google.com/u/0/uc?id=0B1jY75xGiy7eNjJuRy1KWjRieVU&export=download
bzip2 -d all_nodes.bz2
```

Polygons generation from 1K to 10M:

```
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 1000 --num_file 2  # 1K
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 10000 --num_file 2 # 10K
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 100000 --num_file 2  # 100K
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 1000000 --num_file 2  # 1M
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 10000000 --num_file 2  # 10M
```

Points generation from 1K to 10M:

```
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/all_nodes --out_file_dir ../generated_data/ --obj_type point --num_obj 1000 --num_file 1 # 1K 
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/all_nodes --out_file_dir ../generated_data/ --obj_type point --num_obj 10000 --num_file 1 # 10K 
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/all_nodes --out_file_dir ../generated_data/ --obj_type point --num_obj 100000 --num_file 1 # 100K 
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/all_nodes --out_file_dir ../generated_data/ --obj_type point --num_obj 1000000 --num_file 1 # 1M 
python process_OSM.py --input_file_dir /mnt/scratch/wenqi/all_nodes --out_file_dir ../generated_data/ --obj_type point --num_obj 10000000 --num_file 1 # 10M 
```