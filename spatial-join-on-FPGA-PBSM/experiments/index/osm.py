from collect import perform_experiments, parse_arguments, print_arguments

if __name__ == "__main__":
    experiments = [
        {
            "input1": "../../../../data/osm/bin/OSM_50000_polygon_file_0.bin",
            "input2": "../../../../data/tree/tree_OSM_100000_polygon_file_1.bin",
            "description": "OSM, 50K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 60240"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_100000_polygon_file_0.bin",
            "input2": "../../../../data/tree/tree_OSM_100000_polygon_file_1.bin",
            "description": "OSM, 100K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 120040"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_200000_polygon_file_0.bin",
            "input2": "../../../../data/tree/tree_OSM_200000_polygon_file_1.bin",
            "description": "OSM, 200K x 200K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 230240"
        },
        {
            "input1": "../../../../data/osm/bin/OSM_1000000_polygon_file_0.bin",
            "input2": "../../../../data/tree/tree_OSM_1000000_polygon_file_1.bin",
            "description": "OSM, 1M x 1M",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 1200240"
        },
    ]

    base_path = "../../designs/index/simple"
    output_file = "osm.json"

    args = parse_arguments()
    print_arguments(args, experiments, base_path, output_file)

    raw_results = perform_experiments(base_path, experiments, output_file, args)

