from collect import perform_experiments, parse_arguments, print_arguments

if __name__ == "__main__":
    experiments = [
        {
            "input1": "../../../../data/uniform/bin/uniform_10000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/uniform/bin/uniform_10000_polygon_file_1_set_0.bin",
            "description": "Uniform, 10K x 10K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "10240"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_50000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/uniform/bin/uniform_50000_polygon_file_1_set_0.bin",
            "description": "Uniform, 50K x 50K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "1024"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_50000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/uniform/bin/uniform_100000_polygon_file_1_set_0.bin",
            "description": "Uniform, 50K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "102400"
        },
        {
            "input1": "../../../../data/uniform/bin/uniform_100000_polygon_file_0_set_0.bin",
            "input2": "../../../../data/uniform/bin/uniform_100000_polygon_file_1_set_0.bin",
            "description": "Uniform, 100K x 100K",
            "num_processing_units": [1, 2, 4, 8, 16],
            "additional_args": "102400"
        },
    ]

    base_path = "../../designs/nested/simple"
    output_file = "uniform.json"

    args = parse_arguments()
    print_arguments(args, experiments, base_path, output_file)

    raw_results = perform_experiments(base_path, experiments, output_file, args)

