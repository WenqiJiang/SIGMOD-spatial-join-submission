# PostGIS experiments

The PostGIS experiments were carried out in a Docker container utilizing the official PostGIS docker image.

## Setup

### Container creation

The container creation process is straighforward and easily reproducible by following the instructions at the [PostGIS Docker Hub Page](https://registry.hub.docker.com/r/postgis/postgis/), namely the **Usage** section.

### Data transfer

Once the data generation scripts have been ran and the container created, create a directory to store the files on the container. This can be done by running:
```bash
docker exec -ti <container_name> bash
cd home
mkdir data
```

Then use the `docker cp` command to copy the necessary files over to the container, eg:

```bash
docker cp generated_data/postgis_OSM_1000000_polygon_file_0.csv <container_name>:/home/data
```

### Timing on the container

To get timing information add the line `\timing` to `/etc/postgresql-common/psqlrc` on the container.

## Running the experiments

First copy the files in this folder into the home directory of the container. Then to run all the experiments issue the following commands in a shell on the container:

```bash
python3 postgis_run_all_experiments.py \
--out_json_fname postgis_runs.json \
--overwrite 0 \
--num_runs 3
```