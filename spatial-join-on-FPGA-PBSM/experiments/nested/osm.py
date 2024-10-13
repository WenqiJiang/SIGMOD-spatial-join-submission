from collect import perform_experiments, parse_arguments, print_arguments

if __name__ == "__main__":
    experiments = [
        {
            "input1": "../../../../data/osm/bin/OSM_10000_polygon_file_0.bin",
            "input2": "../../../../data/osm/bin/OSM_10000_polygon_file_1.bin",
            "description": "OSM, 10K x 10K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "10240"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_50000_polygon_file_0.bin",
            "input2": "../../../../data/osm/bin/OSM_50000_polygon_file_1.bin",
            "description": "OSM, 50K x 50K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "1024"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_50000_polygon_file_0.bin",
            "input2": "../../../../data/osm/bin/OSM_100000_polygon_file_1.bin",
            "description": "OSM, 50K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "102400"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_100000_polygon_file_0.bin",
            "input2": "../../../../data/osm/bin/OSM_100000_polygon_file_1.bin",
            "description": "OSM, 100K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "102400"
        },
    ]

    base_path = "../../designs/nested/simple"
    output_file = "osm.json"

    args = parse_arguments()
    print_arguments(args, experiments, base_path, output_file)

    raw_results = perform_experiments(base_path, experiments, output_file, args)

