### OSM Data Processing Script

This script processes OpenStreetMap (OSM) building or node datasets and converts them into a rectangular or point format.

#### Usage

```
python process_OSM.py --input_file_dir <<<input_directory>>> --out_file_dir <<<output_directory>>> --obj_type <<<polygon_or_point>>> --num_obj <<<number_of_objects>>> --num_file <<<number_of_files>>>
```

- `--input_file_dir`: Directory containing the input OSM file.
- `--out_file_dir`: Directory where the output files will be saved.
- `--obj_type`: Type of object to process (`polygon` or `point`).
- `--num_obj`: Number of output entries/objects to process (default: 1e8)
- `--num_file`: Number of output files to generate, each containing `num_obj` objects (default: 2)

#### Example

```
python process_OSM.py --input_file_dir /data/buildings --out_file_dir ../generated_data/ --obj_type polygon --num_obj 1000 --num_file 2
```

This example processes the OSM buildings dataset and outputs 1000 polygons in 2 files.