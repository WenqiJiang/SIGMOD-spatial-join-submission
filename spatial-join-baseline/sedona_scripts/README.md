# Sedona experiments

The Apached Sedona experiments were carried out in a Docker container utilizing a Sedona Docker image.

## Setup

### Container creation

The container was created using the image at [Sedona Docker image](https://hub.docker.com/r/abxda/geobigdata) as it contained the necessary tools. Once the container is up and running there are two other dependencies that need to be resolved before running the experiments. The following jars need to be added to `/usr/local/spark/jars`:

+ [Sedona Python Adapter](https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar)
+ [Geotools Wrapper](https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/geotools-24.0/geotools-wrapper-geotools-24.0.jar)

### Data transfer

Once the data generation scripts have been ran and the container created, create a directory to store the files on the container. This can be done by running:
```bash
docker exec -ti <container_name> bash
cd /home
mkdir data
```

Then use the `docker cp` command to copy the necessary files over to the container, eg:

```bash
docker cp generated_data/postgis_OSM_1000000_polygon_file_0.csv <container_name>:/home/data
```

Note: the Sedona experiments reuse the same files as the PostGIS experiments

## Running the experiments

First copy the files in this folder into the home directory of the container. Then to run all the experiments issue the following commands in a shell on the container:

```bash
python sedona_run_all_experiments.py \
--out_json_fname sedona_runs.json \
--overwrite 0 \
--num_runs 3
```