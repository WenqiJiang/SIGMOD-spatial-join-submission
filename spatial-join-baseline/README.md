# Spatial-Join-Baseline

Official repository for the "Supercharge R-trees: Spatial Joins on Modern Hardware" paper baselines.

## Repo structure

This repository is organized as follows:

+ cpp: C++ source and header files for single- and multithreaded implementations

+ cpp_scripts: Python scripts to run C++ experiments and record performance in JSON format

+ cuspatial_scripts: Python scripts to run cuSpatial experiments and record performance in JSON format

+ data_gen: data generation Python scripts for both Uniform and OSM datasets

+ fpga_scripts: Python script to evaluate FPGA resource consumption

+ generated_data: suggested output dir for the data_gen scripts

+ plots: dirs containing the JSON data for the various baselines, along with generated images for the paper and the Python scripts used

+ postgis_scripts: Python scripts to run PostGIS experiments and record performance in JSON format

+ python: Python files describing index structure, traversal algorithms, and other utility methods

+ sedona_scripts: Python scripts to run Apache Sedona experiments and record performance in JSON format

+ spark_scripts: Python scripts to run SpatialSpark experiments and record performance in JSON format

## Authors

Wenqi Jiang, Martin Parvanov