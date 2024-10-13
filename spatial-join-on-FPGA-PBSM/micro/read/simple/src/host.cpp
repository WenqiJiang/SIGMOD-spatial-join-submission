#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "host.hpp"
#include "types.hpp"
#include "utils.cpp"

int main(int argc, char** argv) {
    // OCL error
    cl_int err;

    // process arguments

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <xclbin_path> <num_pages>" << std::endl;
        return EXIT_FAILURE;
    }

    int num_pages = atoi(argv[2]);

    std::cout << "Reading arguments: DONE!" << std::endl;

    // generate pages
    std::srand(std::time(nullptr));
    std::vector<page_t, aligned_allocator<page_t>> dataset(num_pages);

    for (int i = 0; i < num_pages; i++) {
        for (int j = 0; j < 64; j++) {
            // a random byte
            dataset[i][j] = static_cast<uint8_t>(std::rand() % 256);
        }
    }

    std::cout << "Data generation: DONE!" << std::endl;

    // set up OpenCL and kernel

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

    OCL_CHECK(err, cl::Buffer buffer_dataset(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, dataset.size() * sizeof(page_t), dataset.data(), &err));

    std::vector<ap_uint<64>, aligned_allocator<ap_uint<64>>> results(1024);
    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, results.size() * sizeof(ap_uint<64>), results.data(), &err));

    // set kernel arguments

    OCL_CHECK(err, err = kernel.setArg(0, buffer_dataset));
    OCL_CHECK(err, err = kernel.setArg(1, static_cast<int>(num_pages)));
    OCL_CHECK(err, err = kernel.setArg(2, buffer_out));

    std::cout << "Starting processing...\n";

    auto start = std::chrono::high_resolution_clock::now();

    // copy input data to device
    OCL_CHECK(err, err = queue.enqueueMigrateMemObjects({buffer_out}, 0));

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
