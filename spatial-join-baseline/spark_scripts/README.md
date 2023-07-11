# SpatialSpark experiments

The SpatialSpark experiments were carried out in a Docker container utilizing an Ubuntu Docker image.

## Setup

### Container creation

After starting an Ubuntu Docker container clone the [SpatialSpark repo](https://github.com/syoummer/SpatialSpark) and also install Apache Spark 2.0.2. Then replace the following two files with their modified counterparts from this directory:

+ SpatialSpark/src/main/scala/spatialspark/main/SpatialJoinApp.scala
+ SpatialSpark/src/main/scala/spatialspark/join/PartitionedSpatialJoin.scala

### Data transfer

Once the data generation scripts have been ran and the container created, create a directory to store the files on the container. This can be done by running:
```bash
docker exec -ti <container_name> bash
cd /home/SpatialSpark
mkdir data
```

Then use the `docker cp` command to copy the necessary files over to the container, eg:

```bash
docker cp generated_data/spatialspark_OSM_1000000_polygon_file_0.txt <container_name>:/home/SpatialSpark/data
```

## Running the experiments

To run all the experiments issue the following commands in a shell on the container:

```bash
python3 spark_run_all_experiments.py \
--out_json_fname spark_runs.json \
--overwrite 0 \
--num_runs 3
```