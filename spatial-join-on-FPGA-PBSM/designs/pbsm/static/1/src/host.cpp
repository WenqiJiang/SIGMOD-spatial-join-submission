#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "host.hpp"
#include "types.hpp"
#include "utils.hpp"

#include "partition_multi.hpp"

int main(int argc, char **argv) {
    // OCL error
    cl_int err;

    // STEP 1: Process arguments.

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <xclbin_path> <data_file_A> <data_file_B> <opt: max_num_results> <opt: num_initial_partitions_1d> <opt: max_comparisons_per_partition>"
                  << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Reading arguments..." << std::endl;

    std::string data_file_a = argv[2];
    std::string data_file_b = argv[3];

    long max_num_results = 1024;
    if (argc > 4) {
        max_num_results = max_num_results + std::stoi(argv[4]);
    }

    int num_partitions_1d = 10;
    if (argc > 5) {
        num_partitions_1d = atoi(argv[5]);
    }

    int max_comparisons_per_partition = 1000;
    if (argc > 6) {
        max_comparisons_per_partition = atoi(argv[6]);
    }

    int num_threads = N_JOIN_UNITS;

    std::cout << "Num threads: " << num_threads << std::endl;
    std::cout << "Max num results: " << max_num_results << std::endl;
    std::cout << "Num initial partitions (1 dimension): " << num_partitions_1d << std::endl;
    std::cout << "Max comparisons per partition: " << max_comparisons_per_partition << std::endl;

    std::cout << "Reading arguments: DONE!" << std::endl;

    // STEP 2: Reading input files.

    std::cout << "Reading files..." << std::endl;

    // read files
    std::vector <obj_t> dataset_a = read_objects_from_file(data_file_a);
    std::vector <obj_t> dataset_b = read_objects_from_file(data_file_b);

    std::cout << "Size A: " << dataset_a.size() << std::endl;
    std::cout << "Size B: " << dataset_b.size() << std::endl;

    std::cout << "Reading files: DONE!" << std::endl;

    // STEP 3: Partitioning.

    auto partition_result = host_partition(
            dataset_a,
            dataset_b,
            num_partitions_1d,
            max_comparisons_per_partition,
            num_threads
    );

    // STEP 4: Set up OpenCL and kernel.

    std::cout << std::endl << "Searching for devices..." << std::endl;

    std::vector <cl::Device> devices = get_devices();
    cl::Device device = devices[0];
    std::string device_name = device.getInfo<CL_DEVICE_NAME>();
    std::cout << "Device found: " << device_name.c_str() << "!" << std::endl;

    std::cout << std::endl << "Initialising program..." << std::endl;

    // context and command queue
    cl::Context context(device);
    cl::CommandQueue queue(context, device);

    // xclbin bitstream
    xclbin_file_name = argv[1];
    cl::Program::Binaries executor_bins = import_binary_file();

    // program and kernel
    devices.resize(1);
    cl::Program program(context, devices, executor_bins);
    cl::Kernel kernel(program, "executor");

    std::cout << "Initialising program: DONE!" << std::endl;

    // load input and output buffers
    OCL_CHECK(err, cl::Buffer buffer_dataset_A(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY,
                                               partition_result.partitions_A.size() * sizeof(page_t),
                                               partition_result.partitions_A.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_meta_A(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY,
                                            partition_result.meta_A.size() * sizeof(page_t),
                                            partition_result.meta_A.data(), &err));

    OCL_CHECK(err, cl::Buffer buffer_dataset_B(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY,
                                               partition_result.partitions_B.size() * sizeof(page_t),
                                               partition_result.partitions_B.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_meta_B(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY,
                                            partition_result.meta_B.size() * sizeof(page_t),
                                            partition_result.meta_B.data(), &err));

    size_t out_bytes = max_num_results * sizeof(int64_t);
    std::vector <int64_t, aligned_allocator<int64_t>> results(out_bytes / sizeof(int64_t));

    std::cout << "Output bytes: " << out_bytes << std::endl;
    std::cout << "Results size: " << results.size() << std::endl;

    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, out_bytes, results.data(), &err));

    // set kernel arguments
    OCL_CHECK(err, err = kernel.setArg(0, buffer_dataset_A));
    OCL_CHECK(err, err = kernel.setArg(1, buffer_meta_A));
    OCL_CHECK(err, err = kernel.setArg(2, static_cast<int>(partition_result.stats.stats_A.total_objects)));
    OCL_CHECK(err, err = kernel.setArg(3, static_cast<int>(partition_result.partitions_A.size())));

    OCL_CHECK(err, err = kernel.setArg(4, buffer_dataset_B));
    OCL_CHECK(err, err = kernel.setArg(5, buffer_meta_B));
    OCL_CHECK(err, err = kernel.setArg(6, static_cast<int>(partition_result.stats.stats_B.total_objects)));
    OCL_CHECK(err, err = kernel.setArg(7, static_cast<int>(partition_result.partitions_B.size())));

    OCL_CHECK(err, err = kernel.setArg(8, static_cast<int>(partition_result.stats.num_partitions)));
    OCL_CHECK(err, err = kernel.setArg(9, static_cast<int>(
            (partition_result.stats.num_partitions + MAX_META_PER_PAGE - 1) / MAX_META_PER_PAGE)));

    OCL_CHECK(err, err = kernel.setArg(10, buffer_out));

    std::cout << "Starting processing...\n";

    auto start = std::chrono::high_resolution_clock::now();

    // copy input data to device
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({
                                                                buffer_dataset_A,
                                                                buffer_meta_A,
                                                                buffer_dataset_B,
                                                                buffer_meta_B,
                                                                buffer_out
                                                        }, 0));

    queue.finish();

    auto kernel_start = std::chrono::high_resolution_clock::now();

    // launch the kernel
    OCL_CHECK(err, err = queue.enqueueTask(kernel));
    queue.finish();

    auto kernel_end = std::chrono::high_resolution_clock::now();

    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({buffer_out}, CL_MIGRATE_MEM_OBJECT_HOST));
    
    queue.finish();

    // stop timer
    auto end = std::chrono::high_resolution_clock::now();

    double duration_kernel =
            (std::chrono::duration_cast<std::chrono::microseconds>(kernel_end - kernel_start).count()) / 1000.0;
    double duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000.0;

    std::cout << std::endl << "Processing: DONE!\n";

    printf("TIME[kernel_time]: %.2lf ms.\n", duration_kernel);
    printf("TIME[total_time]: %.2lf ms.\n", duration);

    printf("TIME[cpu_init_time]: %.2lf ms.\n", partition_result.init_grid_time);
    printf("TIME[cpu_partition_time]: %.2lf ms.\n", partition_result.partition_objects_time);
    printf("TIME[cpu_refine_time]: %.2lf ms.\n", partition_result.refine_partitions_time);
    printf("TIME[cpu_prepare_time]: %.2lf ms.\n", partition_result.prepare_partitions_time);
    printf("TIME[cpu_time]: %.2lf ms.\n", partition_result.init_grid_time + partition_result.partition_objects_time +
                                          partition_result.refine_partitions_time +
                                          partition_result.prepare_partitions_time);

    printf("RESULTS: %ld\n", results[0]);
    std::cout << "COMPLETED!\n";

    return 0;
}
