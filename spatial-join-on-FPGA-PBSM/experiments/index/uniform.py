from collect import perform_experiments, parse_arguments, print_arguments

if __name__ == "__main__":
    experiments = [
        {
            "input1": "../../../../data/uniform/bin/uniform_50000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/tree/tree_uniform_100000_polygon_file_1_set_0.bin",
            "description": "Uniform, 50K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 60240"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_100000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/tree/tree_uniform_100000_polygon_file_1_set_0.bin",
            "description": "Uniform, 100K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 120040"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_200000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/tree/tree_uniform_200000_polygon_file_1_set_0.bin",
            "description": "Uniform, 200K x 200K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 230240"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_1000000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/tree/tree_uniform_1000000_polygon_file_1_set_0.bin",
            "description": "Uniform, 1M x 1M",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "2 16 1200240"
        },
    ]

    base_path = "../../designs/index/simple"
    output_file = "uniform.json"

    args = parse_arguments()
    print_arguments(args, experiments, base_path, output_file)

    raw_results = perform_experiments(base_path, experiments, output_file, args)

