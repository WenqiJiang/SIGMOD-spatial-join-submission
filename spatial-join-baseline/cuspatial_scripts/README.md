# CuSpatial

## Install

```
conda create -n cuspatial  -c rapidsai -c conda-forge -c nvidia cuspatial=23.04 python=3.10 cudatoolkit=11.8
conda activate cuspatial
conda install -c anaconda ipykernelconda install -c anaconda ipykernel # if using ipynb
```

The older version works but does not match their documents (23.04 at the date of May 16 2023):

```
conda create -n rapids-22.08 -c rapidsai -c conda-forge -c nvidia cuspatial=22.08 python=3.9 cudatoolkit=11.5
```

Their docker image does not seem to work on the sgs-gpu machine.
## Tutorial

The `official_examples` folder contains a set of examples from the official documents about spatial join: 

https://docs.rapids.ai/api/cuspatial/stable/user_guide/cuspatial_api_examples/#Geopandas-and-cuDF-integration

The example_join.ipynb provides a set of examples we write. 

## Run 

### Run all experiment

Batch size: 
* A100 SXM (40 GB) on lambdalab: 10K succeed, 50K failed OSM1Mx1M, 100K failed
* 3090 Ti (24 GB): 10K succeed; 50K succeed in most cases but failed in some OSM cases, 100K failed

Now it takes ~8 hours to run all the experiments, primarily because of the non-core logic including loading the data to geopandas and get the boundaries of the datasets.

```
conda activate cuspatial

python cuspatial_run_all_experiments.py \
--out_json_fname GPU_perf_3_runs_batch_10K.json \
--overwrite 0 \
--python_dir ./pip_join.py \
--data_dir ../generated_data \
--num_runs 3 \
--avg_leaf_size 16 \
--batch_size 10000
```

The OSM 1M points x 100K/1M/10M polygons consume the most memory, allowing a maximum batch size of 10K even on A100:

```
python ./pip_join.py --point_file_dir ../generated_data/cuspatial_OSM_1000000_point_file_0.csv --polygon_file_dir ../generated_data/cuspatial_OSM_1000000_polygon_file_0.csv --avg_leaf_size 16 --batch_size 10000
```

### Run one experiment


```
conda activate cuspatial

python pip_join.py --point_file_dir ../generated_data/cuspatial_OSM_100000_point_file_0.csv --polygon_file_dir ../generated_data/cuspatial_OSM_100000_polygon_file_0.csv --avg_leaf_size 128 --batch_size 1000
```

### Measure Energy

Using two datasets: OSM 10M and Uniform 10M.

On one hand, run the GPU join in a while loop.

```
python ./pip_join.py --point_file_dir ../generated_data/cuspatial_OSM_10000000_point_file_0.csv --polygon_file_dir ../generated_data/cuspatial_OSM_10000000_polygon_file_0.csv --avg_leaf_size 16 --batch_size 10000 --measure_energy 1


python ./pip_join.py --point_file_dir ../generated_data/cuspatial_uniform_10000000_point_file_0.csv --polygon_file_dir ../generated_data/cuspatial_uniform_10000000_polygon_file_0_set_0.csv --avg_leaf_size 16 --batch_size 10000 --measure_energy 1
```

On the other hand, print out GPU log, should measure at least one minute:

```
nvidia-smi -l 1 > log_energy_OSM
nvidia-smi -l 1 > log_energy_Uniform
```

Compute the average energy:

```
python compute_average_energy.py --gpu_id 0 --nvidia_smi_file log_energy_OSM_batch_10K 
```

OSM: Average energy consumption: 95.01754385964912 W

Uniform: Average energy consumption: 95.1063829787234 W

### The effect of average leaf node sizes and batch sizes

average leaf node sizes: controls the levels of the tree. The level must guarantees the achieved average leaf size < user-specified average leaf node sizes.

The batch sizes is related to memory usage. cuSpatial tends to over-allocate memory, thus the maximum batch size should be tested on specific GPUs. The main memory consumption consumes from run-time memory allocation - loading one 10M dataset with index only consumes ~1GB GPU memory. 10K batch size work, but 100K will fail.


python pip_join.py --point_file_dir ../generated_data/cuspatial_OSM_100000_point_file_0.csv --polygon_file_dir ../generated_data/cuspatial_OSM_100000_polygon_file_0.csv --avg_leaf_size 128 --batch_size 1000

#### OSM 100K x 100K 

Batch size effect

--avg_leaf_size 128 --batch_size 1000

time index: 297.90496826171875 ms
time join: 4753.384828567505 ms

--avg_leaf_size 128 --batch_size 10000

time index: 17.076492309570312 ms
time join: 1027.2297859191895 ms

--avg_leaf_size 128 --batch_size 20000

time index: 16.382217407226562 ms
time join: 531.4815044403076 ms

--avg_leaf_size 128 --batch_size 50000
time index: 16.891956329345703 ms
time join: 402.8751850128174 ms

--avg_leaf_size 128 --batch_size 100000 -> 100K out of memory

Leaf size effect

--avg_leaf_size 2 --batch_size 10000

time index: 17.37666130065918 ms
time join: 556.9512844085693 ms

--avg_leaf_size 4 --batch_size 10000
time index: 16.898393630981445 ms
time join: 573.5130310058594 ms

--avg_leaf_size 8 --batch_size 10000

time index: 16.836166381835938 ms
time join: 711.4825248718262 ms

--avg_leaf_size 16 --batch_size 10000

time index: 17.204761505126953 ms
time join: 693.3958530426025 ms

--avg_leaf_size 32 --batch_size 10000

time index: 16.648292541503906 ms
time join: 1021.0225582122803 ms

--avg_leaf_size 64 --batch_size 10000

time index: 17.742156982421875 ms
time join: 1027.0543098449707 ms

--avg_leaf_size 128 --batch_size 10000

time index: 17.076492309570312 ms
time join: 1027.2297859191895 ms

--avg_leaf_size 256 --batch_size 10000

time index: 16.949892044067383 ms
time join: 1032.4761867523193 ms

--avg_leaf_size 512 --batch_size 10000

time index: 16.864299774169922 ms
time join: 1756.9379806518555 ms

--avg_leaf_size 1024 --batch_size 10000

time index: 17.824172973632812 ms
time join: 1747.215986251831 ms

--avg_leaf_size 2048 --batch_size 10000

time index: 17.151832580566406 ms
time join: 2612.967014312744 ms


#### Uniform 100K x 100K

Leaf size effect

--avg_leaf_size 2 --batch_size 10000

time index: 18.15176010131836 ms
time join: 450.04749298095703 ms

--avg_leaf_size 16 --batch_size 10000

time index: 18.109560012817383 ms
time join: 446.3775157928467 ms

--avg_leaf_size 128 --batch_size 10000

time index: 18.073081970214844 ms
time join: 606.555700302124 ms

--avg_leaf_size 1024 --batch_size 10000

time index: 16.856908798217773 ms
time join: 1042.7021980285645 ms

