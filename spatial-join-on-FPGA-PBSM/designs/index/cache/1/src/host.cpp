#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "host.hpp"
#include "types.hpp"
#include "utils.hpp"

int main(int argc, char** argv) {
    // OCL error
    cl_int err;

    // STEP 1: Process arguments.

    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <xclbin_path> <data_file_A> <tree_file_B> <tree_max_level> <tree_max_node_entries_count> <opt: max_num_results>" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Reading arguments..." << std::endl;

    std::string data_file_a = argv[2];
    std::string tree_file_b = argv[3];

    int tree_max_level = std::stoi(argv[4]);
    int tree_max_node_entries_count = std::stoi(argv[5]);

    // e.g. tree_max_node_entries_count = 16
    // ceil(16 / MAX = 3) = 6
    // 6 + 1 (meta) = 7 pages = 7 * 64 = 448 bytes (tree page size)

    long max_num_results = 1024;
    if (argc > 6) {
        max_num_results = max_num_results + std::stoi(argv[6]);
    }

    std::cout << "Tree max level: " << tree_max_level << std::endl;
    std::cout << "Tree max node entries count: " << tree_max_node_entries_count << std::endl;
    std::cout << "Max num of results: " << max_num_results << std::endl;

    std::cout << "Reading arguments: DONE!" << std::endl;

    // STEP 2: Reading input files.

    std::cout << "Reading files..." << std::endl;

    std::cout << "Reading file A..." << std::endl;

    int num_objects_A = 0;
    std::vector<int, aligned_allocator<int>> dataset_a = read_from_binary_file(data_file_a, num_objects_A);
    int num_pages_A = (num_objects_A + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;

    std::cout << "Size A: " << dataset_a.size() << std::endl;
    std::cout << "Num objects of A: " << num_objects_A << std::endl;
    std::cout << "Num pages of A: " << num_pages_A << std::endl;

    std::cout << "Reading dataset A: DONE!" << std::endl;

    // read index tree
    std::cout << "Reading index B..." << std::endl;

    int root_id = 0;
    int64_t index_B_bytes = 0;

    std::vector<int, aligned_allocator<int> > index_B = read_indexed_dataset(tree_file_b, index_B_bytes);

    std::cout << "Max level of B: " << tree_max_level << std::endl;
    std::cout << "Max entry count of a node: " << tree_max_node_entries_count << std::endl;

    std::cout << "Root info: " << std::endl;
    std::cout << "  is leaf: " << index_B[0] << std::endl;
    std::cout << "  entry count: " << index_B[1] << std::endl;

    int page_bytes = (1 + (tree_max_node_entries_count + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE) * PAGE_SIZE_BYTES;
    int node_pages = page_bytes / 64;
    std::cout << "Page bytes: " << page_bytes << std::endl;
    std::cout << "Node pages: " << node_pages << std::endl;

    std::cout << "Reading index B: DONE!" << std::endl;

    // STEP 3: Set up OpenCL and kernel.

    std::cout << std::endl << "Searching for devices..." << std::endl;

    std::vector<cl::Device> devices = get_devices();
    cl::Device device = devices[0];
    std::string device_name = device.getInfo<CL_DEVICE_NAME>();
    std::cout << "Device found: " << device_name.c_str() << std::endl;

    std::cout << "Initialising program..." << std::endl;

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
    OCL_CHECK(err, cl::Buffer buffer_a(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, dataset_a.size() * sizeof(int), dataset_a.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_tree_b(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, index_B_bytes, index_B.data(), &err));

    size_t out_bytes = max_num_results * sizeof(int64_t);
    std::vector<int64_t, aligned_allocator<int64_t>> results(out_bytes / sizeof(int64_t));

    std::cout << "Output bytes: " << out_bytes << std::endl;
    std::cout << "Results size: " << results.size() << std::endl;

    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, out_bytes, results.data(), &err));

    // set kernel arguments

    // A
    OCL_CHECK(err, err = kernel.setArg(0, static_cast<int>(num_objects_A)));
    OCL_CHECK(err, err = kernel.setArg(1, static_cast<int>(num_pages_A)));
    OCL_CHECK(err, err = kernel.setArg(2, buffer_a));

    OCL_CHECK(err, err = kernel.setArg(3, int(tree_max_level)));
    OCL_CHECK(err, err = kernel.setArg(4, int(root_id)));
    OCL_CHECK(err, err = kernel.setArg(5, int(node_pages)));
    OCL_CHECK(err, err = kernel.setArg(6, int(tree_max_node_entries_count)));
    OCL_CHECK(err, err = kernel.setArg(7, buffer_tree_b));
    OCL_CHECK(err, err = kernel.setArg(8, buffer_out));

    std::cout << "Starting processing...\n";

    auto start = std::chrono::high_resolution_clock::now();

    // copy input data to device
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({
        buffer_a,
        buffer_tree_b,
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

    double duration_kernel = (std::chrono::duration_cast<std::chrono::microseconds>(kernel_end - kernel_start).count()) / 1000.0;
    double duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000.0;

    std::cout << "Processing: DONE!\n";
    printf("TIME[kernel_time]: %.2lf ms.\n", duration_kernel);
    printf("TIME[total_time]: %.2lf ms.\n", duration);
    printf("RESULTS: %ld\n", results[0]);
    std::cout << "COMPLETED!\n";

    return 0;
}