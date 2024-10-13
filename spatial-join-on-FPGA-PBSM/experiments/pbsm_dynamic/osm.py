from collect import perform_experiments, parse_arguments, print_arguments

if __name__ == "__main__":
    experiments = [
        {
            "input1": "../../../../data/osm/txt/OSM_50000_polygon_file_0.txt",
            "input2": "../../../../data/osm/txt/OSM_50000_polygon_file_1.txt",
            "description": "OSM, 50K x 50K",
            "num_processing_units": [1, 2, 4, 8],
            "additional_args": "60000 10 1000"
        },
        {
            "input1": "../../../../data/osm/txt/OSM_50000_polygon_file_0.txt",
            "input2": "../../../../data/osm/txt/OSM_100000_polygon_file_1.txt",
            "description": "OSM, 50K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "60000 10 1000"
        },
        {
            "input1": "../../../../data/osm/txt/OSM_200000_polygon_file_0.txt",
            "input2": "../../../../data/osm/txt/OSM_200000_polygon_file_1.txt",
            "description": "OSM, 200K x 200K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "120000 10 1000"
        },
        {
            "input1": "../../../../data/osm/txt/OSM_1000000_polygon_file_0.txt",
            "input2": "../../../../data/osm/txt/OSM_1000000_polygon_file_1.txt",
            "description": "OSM, 1M x 1M",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "1200000 10 1000"
        },
    ]

    base_path = "../../designs/pbsm/dynamic"
    output_file = "osm.json"

    args = parse_arguments()
    print_arguments(args, experiments, base_path, output_file)

    raw_results = perform_experiments(base_path, experiments, output_file, args)

