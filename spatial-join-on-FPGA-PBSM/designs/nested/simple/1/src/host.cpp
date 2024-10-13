#include <stdint.h>
#include <chrono>
#include <vector>
#include <iostream>
#include <fstream>
#include <string>

#include "host.hpp"
#include "types.hpp"
#include "utils.hpp"

int main(int argc, char** argv) {
    // OCL error
    cl_int err;

    // STEP 1: Process arguments.

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <xclbin_path> <data_file_A> <data_file_B> <opt: max_num_results = 1024>" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Reading arguments..." << std::endl;

    std::string data_file_a = argv[2];
    std::string data_file_b = argv[3];

    long max_num_results = 1024;
    if (argc > 4) {
        max_num_results = max_num_results + std::stoi(argv[4]);
    }

    std::cout << "Max number of results: " << max_num_results << std::endl;

    std::cout << "Reading arguments: DONE!" << std::endl;

    // STEP 2: Read files.

    std::cout << "Reading files..." << std::endl;

    // read files
    int num_objects_A = 0;
    int num_objects_B = 0;
    std::vector<int, aligned_allocator<int> > dataset_a = read_from_binary_file(data_file_a, num_objects_A);
    std::vector<int, aligned_allocator<int> > dataset_b = read_from_binary_file(data_file_b, num_objects_B);

    int num_pages_A = (num_objects_A + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
    int num_pages_B = (num_objects_B + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;

    std::cout << "Size A: " << dataset_a.size() << std::endl;
    std::cout << "Size B: " << dataset_b.size() << std::endl;

    std::cout << "Num obj A: " << num_objects_A << std::endl;
    std::cout << "Num obj B: " << num_objects_B << std::endl;

    std::cout << "Reading files: DONE!" << std::endl;

    // STEP 3: Set up OpenCL and kernel.

    std::cout << "Searching for devices..." << std::endl;

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
    OCL_CHECK(err, cl::Buffer buffer_b(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, dataset_b.size() * sizeof(int), dataset_b.data(), &err));

    size_t out_bytes = max_num_results * sizeof(int64_t);
    std::vector<int64_t, aligned_allocator<int64_t>> results(out_bytes / sizeof(int64_t));

    std::cout << "Output bytes: " << out_bytes << std::endl;
    std::cout << "Results size: " << results.size() << std::endl;

    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, out_bytes, results.data(), &err));

    // set kernel arguments
    kernel.setArg(0, static_cast<int>(num_objects_A));
    kernel.setArg(1, static_cast<int>(num_pages_A));
    kernel.setArg(2, buffer_a);
    kernel.setArg(3, static_cast<int>(num_objects_B));
    kernel.setArg(4, static_cast<int>(num_pages_B));
    kernel.setArg(5, buffer_b);
    kernel.setArg(6, buffer_out);

    std::cout << "Starting processing...\n";

    auto start = std::chrono::high_resolution_clock::now();

    // copy input data to device
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({
        buffer_a,
        buffer_b,
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