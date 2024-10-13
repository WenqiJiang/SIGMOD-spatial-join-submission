#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <stdlib.h>

#include "host.hpp"
#include "types.hpp"
#include "utils.cpp"

int main(int argc, char** argv) {
    // OCL error
    cl_int err;

    // STEP 1: Process arguments.

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <xclbin_path> <opt: num_partitions> <opt: num_objs_per_partition> <opt: intersection_percentage>" << std::endl;
        return EXIT_FAILURE;
    }

    int num_partitions = 10;
    if (argc > 2) {
        num_partitions = atoi(argv[2]);
    }

    int num_objs_per_partition = 16;
    if (argc > 3) {
        num_objs_per_partition = atoi(argv[3]);
    }

    float intersection_percentage = 1.0;
    if (argc > 4) {
        intersection_percentage = atof(argv[4]);
    }

    printf("Number of partitions: %d\n", num_partitions);
    printf("Number of objects per partition: %d\n", num_objs_per_partition);
    printf("Intersection percentage: %.2f\n", intersection_percentage);

    std::cout << "Reading arguments: DONE!" << std::endl;

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

    size_t out_bytes = 1024 * sizeof(int64_t);
    std::vector<int64_t, aligned_allocator<int64_t>> results(out_bytes / sizeof(int64_t));
    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, out_bytes, results.data(), &err));

    // set kernel arguments

    OCL_CHECK(err, err = kernel.setArg(0, static_cast<int>(num_partitions)));
    OCL_CHECK(err, err = kernel.setArg(1, static_cast<int>(num_objs_per_partition)));
    OCL_CHECK(err, err = kernel.setArg(2, intersection_percentage));
    OCL_CHECK(err, err = kernel.setArg(3, buffer_out));

    // copy input data to device
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({buffer_out}, 0));

    std::cout << "Launching kernel...\n";

    // launch the kernel
    OCL_CHECK(err, err = queue.enqueueTask(kernel));
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({buffer_out}, CL_MIGRATE_MEM_OBJECT_HOST));
    queue.finish();

    printf("RESULTS: %ld\n", results[0]);
    std::cout << "COMPLETED!\n";

    return 0;
}
