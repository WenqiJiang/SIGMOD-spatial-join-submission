#include <vector>
#include <algorithm>
#include <utility>
#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <chrono>
#include <cmath>
#include <array>
#include <cstdint>
#include <cstring>

#include "partition_utils.hpp"
#include "partition_multi.hpp"
#include "types.hpp"
#include "utils.hpp"


int main(int argc, char** argv) {
    std::cout << "Version: Parallel. " << std::endl;

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_file_a> <data_file_b> [num_partitions_1d = 10] [max_comparisons_per_partition = 1000] [num_threads = 1]" << std::endl;
        return 1;
    }

    int num_partitions_1d = (argc >= 4) ? std::atoi(argv[3]) : 10;
    int max_comparisons_per_partition = (argc >= 5) ? std::atoi(argv[4]) : 1000;
    int num_threads = (argc >= 6) ? std::atoi(argv[5]) : 1;

    std::cout << "Reading files..." << std::endl;

    std::string data_file_a = argv[1];
    std::string data_file_b = argv[2];

    // read files
    std::vector<obj_t> dataset_a = read_objects_from_file(data_file_a);
    std::vector<obj_t> dataset_b = read_objects_from_file(data_file_b);

    std::cout << "Size A: " << dataset_a.size() << std::endl;
    std::cout << "Size B: " << dataset_b.size() << std::endl;

    std::cout << "Reading files: DONE!" << std::endl;

    std::cout << "Num partitions (1d): " << num_partitions_1d << std::endl;
    std::cout << "Max comparisons per partition: " << max_comparisons_per_partition << std::endl;
    std::cout << "Num threads: " << num_threads << std::endl;

    auto partition_result = host_partition(
            dataset_a,
            dataset_b,
            num_partitions_1d,
            max_comparisons_per_partition,
            num_threads
//            true
    );

    print_results(partition_result);

    return 0;
}
