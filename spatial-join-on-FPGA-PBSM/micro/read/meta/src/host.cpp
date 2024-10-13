#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "host.hpp"
#include "types.hpp"
#include "utils.cpp"


// Function to generate dataset and metadata
void generate_dataset_and_meta(int num_pages_data,
                               int num_pages_meta,
                               std::vector<page_t, aligned_allocator<page_t>>& dataset,
                               std::vector<page_t, aligned_allocator<page_t>>& metadata) {
    // initialise random seed
    std::srand(std::time(nullptr));

    // generate the dataset pages
    dataset.resize(num_pages_data);
    for (int i = 0; i < num_pages_data; i++) {
        // 64 bytes per page (512 bits)
        for (int j = 0; j < 64; j++) {
            // fill with random bytes
            dataset[i][j] = static_cast<uint8_t>(std::rand() % 256);
        }
    }

    // gen the metadata pages
    metadata.resize(num_pages_meta);
    for (int i = 0; i < num_pages_meta; i++) {
        for (int j = 0; j < 64; j++) {
            metadata[i][j] = static_cast<uint8_t>(std::rand() % 256);
        }
    }
}


int main(int argc, char** argv) {
    // OCL error
    cl_int err;

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <xclbin_path> <num_pages_meta> <num_pages_data>" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Reading arguments..." << std::endl;

    // read arguments
    int num_pages_meta = atoi(argv[2]);
    int num_pages_data = atoi(argv[3]);

    std::cout << "num_pages_meta: " << num_pages_meta << std::endl;
    std::cout << "num_pages_data: " << num_pages_data << std::endl;

    std::cout << "Reading arguments: DONE!" << std::endl;

    // generate datasets and metadata
    std::vector<page_t, aligned_allocator<page_t>> partitions_A;
    std::vector<page_t, aligned_allocator<page_t>> meta_A;
    generate_dataset_and_meta(num_pages_data, num_pages_meta, partitions_A, meta_A);

    std::vector<page_t, aligned_allocator<page_t>> partitions_B;
    std::vector<page_t, aligned_allocator<page_t>> meta_B;
    generate_dataset_and_meta(num_pages_data, num_pages_meta, partitions_B, meta_B);

    // setup openCL device, kernel etc

    // OPENCL HOST CODE AREA START

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

    std::cout << "Loading bitstream: DONE.\n";

    // load input and output buffers
    OCL_CHECK(err, cl::Buffer buffer_dataset_A(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, partitions_A.size() * sizeof(page_t), partitions_A.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_meta_A(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, meta_A.size() * sizeof(page_t), meta_A.data(), &err));

    OCL_CHECK(err, cl::Buffer buffer_dataset_B(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, partitions_B.size() * sizeof(page_t), partitions_B.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_meta_B(context, CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, meta_B.size() * sizeof(page_t), meta_B.data(), &err));

    std::vector<ap_uint<64>, aligned_allocator<ap_uint<64>>> results(1024);
    OCL_CHECK(err, cl::Buffer buffer_out(context, CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, results.size() * sizeof(ap_uint<64>), results.data(), &err));

    // set kernel arguments
    OCL_CHECK(err, err = kernel.setArg(0, buffer_dataset_A));
    OCL_CHECK(err, err = kernel.setArg(1, buffer_meta_A));
    OCL_CHECK(err, err = kernel.setArg(2, static_cast<int>(partitions_A.size())));

    OCL_CHECK(err, err = kernel.setArg(3, buffer_dataset_B));
    OCL_CHECK(err, err = kernel.setArg(4, buffer_meta_B));
    OCL_CHECK(err, err = kernel.setArg(5, static_cast<int>(partitions_B.size())));

    OCL_CHECK(err, err = kernel.setArg(6, static_cast<int>(num_pages_meta)));

    OCL_CHECK(err, err = kernel.setArg(7, buffer_out));

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

    double duration_kernel = (std::chrono::duration_cast<std::chrono::microseconds>(kernel_end - kernel_start).count()) / 1000.0;
    double duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000.0;

    std::cout << "Processing: DONE!\n";
    printf("TIME[kernel_time]: %.2lf ms.\n", duration_kernel);
    printf("TIME[total_time]: %.2lf ms.\n", duration);
    printf("RESULTS: %ld\n", results[0]);
    std::cout << "COMPLETED!\n";

    return 0;
}
