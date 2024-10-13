/*
Example Usage: 
./host xclbin/vadd.hw.xclbin /mnt/scratch/wenqi/spatial-join-baseline/cpp/tree_A.bin /mnt/scratch/wenqi/spatial-join-baseline/cpp/tree_B.bin 3 3 128 40428

std::cout << "Usage: " << argv[0] << "<1: xclbin>  <2: TreeBin Dir 1> <3: TreeBin Dir 2> <4: Tree 1 level> " 
	"<5: Tree 2 level> <6: Max entry num in a node> <7: num results>" << std::endl;


*/


#include <stdint.h>
#include <chrono>
#include <cassert>
#include <regex> 

#include "host.hpp"

#include "constants.hpp"
// Wenqi: seems 2022.1 somehow does not support linking ap_uint.h to host?
// #include "ap_uint.h"

using namespace std;

// boost::filesystem does not compile well, so implement this myself
string dir_concat(string dir1, string dir2) {
    if (dir1.back() != '/') {
        dir1 += '/';
    }
    return dir1 + dir2;
}

void wait_for_enter(const std::string &msg) {
    std::cout << msg << std::endl;
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
}

int main(int argc, char** argv)
{

	if (argc != 9) {
        // Rx bytes = Tx byte (forwarding the data)
        std::cout << "Usage: " << argv[0] << "<1: xclbin>  <2: TreeBin Dir 1> <3: TreeBin Dir 2> <4: Tree 1 level> " 
			"<5: Tree 2 level> <6: Max entry num in a node> <7: page_bytes> <8: num results>" << std::endl;
		std::cout << "Example Usage: ./host xclbin/vadd.hw.xclbin /mnt/scratch/wenqi/spatial-join-baseline/cpp/tree_A.bin "
			"/mnt/scratch/wenqi/spatial-join-baseline/cpp/tree_B.bin 3 3 64 4096 39600";
        exit(1);
    }

    int root_id_A = 0;
    int root_id_B = 0;
    int max_level_A = stoi(argv[4]);
    int max_level_B = stoi(argv[5]);
	assert (max_level_A <= max_level_B);
	int max_entry_num = stoi(argv[6]);   // max number of entries per page (set by CPU)
    int page_bytes = stoi(argv[7]);; // page size -> used in the index bin
	assert (page_bytes <= 4096); // the read input burst FIFO length has a limit
	assert (page_bytes % 64 == 0); // must be aligned with 64-byte AXI
	long sw_num_results = stoi(argv[8]);
	int entry_axi = max_entry_num % 3 == 0? max_entry_num / 3 : max_entry_num / 3 + 1;
	int axi_per_page = 1 + entry_axi;   // number of 64-byte read per node, <= page_bytes, decided by entry_num
	if (axi_per_page * 64 > page_bytes) {
		std::cout << "axi_per_page * 64 > page_bytes";
		exit(1);
	}

    string tree_bin_A_dir = argv[2];
    string tree_bin_B_dir = argv[3];

    // cout << "Parsed tree level A : " << max_level_A << endl;
    // cout << "Parsed tree level B : " << max_level_B << endl;

    // get data size from disk 
    ifstream tree_A_fstream(
        tree_bin_A_dir, ios::in | ios::binary);
    tree_A_fstream.seekg(0, tree_A_fstream.end);
    int64_t tree_A_bytes = tree_A_fstream.tellg();
    tree_A_fstream.seekg(0, tree_A_fstream.beg);
    cout << "Tree A bytes: " << tree_A_bytes << endl;

    ifstream tree_B_fstream(
        tree_bin_B_dir, ios::in | ios::binary);
    tree_B_fstream.seekg(0, tree_B_fstream.end);
    int64_t tree_B_bytes = tree_B_fstream.tellg();
    tree_B_fstream.seekg(0, tree_B_fstream.beg);
    cout << "Tree B bytes: " << tree_B_bytes << endl;

    // allocate memory 
    cout << "Allocating memory...\n";
    size_t bytes_page_A = tree_A_bytes;
    size_t bytes_page_B = tree_B_bytes;
    vector<int ,aligned_allocator<int>> in_pages_A(bytes_page_A / sizeof(int), 0);
    vector<int ,aligned_allocator<int>> in_pages_B(bytes_page_B / sizeof(int), 0);

    size_t layer_cache_bytes = 1 * size_t(1000) * size_t(1000) * size_t(1000); // no more than 16 GB
    cout << "layer_cache_bytes: " << layer_cache_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> layer_cache(layer_cache_bytes / sizeof(int64_t), 0);

    // const int bias = 1 + N_JOIN_PE; // the first number writes total intersection count, the rest write tasks per PE
    size_t out_bytes = (1 + N_JOIN_PE + sw_num_results) * 8 + 1024; // 8 byte per ap_uint<64>, 1024 = extra buffer
    cout << "out_bytes: " << out_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> out(out_bytes / sizeof(int64_t), 0);

    // load data from disk
    tree_A_fstream.read((char*) in_pages_A.data(), tree_A_bytes);
    if (!tree_A_fstream) {
            cout << "error: only " << tree_A_fstream.gcount() << " could be read";
        exit(1);
    }
    tree_B_fstream.read((char*) in_pages_B.data(), tree_B_bytes);
    if (!tree_B_fstream) {
            cout << "error: only " << tree_B_fstream.gcount() << " could be read";
        exit(1);
    }

    cout << "Root A info: " << endl;
    cout << "  is leaf: " << in_pages_A[0] << endl;
    cout << "  count: " << in_pages_A[1] << endl;

    cout << "Root B info: " << endl;
    cout << "  is leaf: " << in_pages_B[0] << endl;
    cout << "  count: " << in_pages_B[1] << endl;

// OPENCL HOST CODE AREA START

    cl_int err;

    vector<cl::Device> devices = get_devices();
    cl::Device device = devices[0];
    string device_name = device.getInfo<CL_DEVICE_NAME>();
    cout << "Found Device=" << device_name.c_str() << endl;

    //Creating Context and Command Queue for selected device
    cl::Context context(device);
    // Wenqi: enable out of order execution
    cl::CommandQueue q(context, device, CL_QUEUE_PROFILING_ENABLE | CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE);

    // Import XCLBIN
    xclbin_file_name = argv[1];
    cl::Program::Binaries vadd_bins = import_binary_file();

    // Program and Kernel
    devices.resize(1);
    auto start_program = chrono::high_resolution_clock::now();
    cl::Program program(context, devices, vadd_bins);
    auto end_program = chrono::high_resolution_clock::now();
    double duration_program = (chrono::duration_cast<chrono::microseconds>(end_program-start_program).count()) / 1000.0;
    printf("FPGA program time: %.2lf ms", duration_program); 
    cl::Kernel krnl_scheduler(program, "scheduler");
    cl::Kernel krnl_executor(program, "executor");

    cout << "Finish loading bitstream...\n";
    // in init 
    OCL_CHECK(err, cl::Buffer buffer_in_pages_A   (context,CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, 
            bytes_page_A, in_pages_A.data(), &err));
    OCL_CHECK(err, cl::Buffer buffer_in_pages_B   (context,CL_MEM_USE_HOST_PTR | CL_MEM_READ_ONLY, 
            bytes_page_B, in_pages_B.data(), &err));
    // in & out
    OCL_CHECK(err, cl::Buffer buffer_layer_cache(context,CL_MEM_USE_HOST_PTR, 
            layer_cache_bytes, layer_cache.data(), &err));
	// out
    OCL_CHECK(err, cl::Buffer buffer_out(context,CL_MEM_USE_HOST_PTR | CL_MEM_WRITE_ONLY, 
            out_bytes, out.data(), &err));

    cout << "Finish allocate buffer...\n";

    // int arg_counter = 0;    
    // in 
    OCL_CHECK(err, err = krnl_scheduler.setArg(0, int(max_level_A)));
    OCL_CHECK(err, err = krnl_scheduler.setArg(1, int(max_level_B)));

    OCL_CHECK(err, err = krnl_executor.setArg(0, int(max_level_A)));
    OCL_CHECK(err, err = krnl_executor.setArg(1, int(max_level_B)));
    OCL_CHECK(err, err = krnl_executor.setArg(2, int(root_id_A)));
    OCL_CHECK(err, err = krnl_executor.setArg(3, int(root_id_B)));
    OCL_CHECK(err, err = krnl_executor.setArg(4, int(page_bytes)));
    OCL_CHECK(err, err = krnl_executor.setArg(5, int(max_entry_num)));
    OCL_CHECK(err, err = krnl_executor.setArg(13, buffer_in_pages_A));
    OCL_CHECK(err, err = krnl_executor.setArg(14, buffer_in_pages_B));
    OCL_CHECK(err, err = krnl_executor.setArg(17, buffer_layer_cache));
    OCL_CHECK(err, err = krnl_executor.setArg(18, buffer_out));


    // Copy input data to device global memory
    OCL_CHECK(err, err = q.enqueueMigrateMemObjects({
        // in
        buffer_layer_cache,
        buffer_out
        },0/* 0 means from host*/));
    q.finish();

    // wait_for_enter("\nPress ENTER to continue after setting up ILA trigger...");
    cout << "Launching kernel...\n";
    // Launch the Kernel
    auto start = chrono::high_resolution_clock::now();
    OCL_CHECK(err, err = q.enqueueMigrateMemObjects({
        // in
        buffer_in_pages_A,
        buffer_in_pages_B,
        },0/* 0 means from host*/));
    q.finish();
    auto t_before_kernel = chrono::high_resolution_clock::now();
    OCL_CHECK(err, err = q.enqueueTask(krnl_scheduler));
    OCL_CHECK(err, err = q.enqueueTask(krnl_executor));
    q.finish();
    auto t_after_kernel = chrono::high_resolution_clock::now();
    
    // Copy Result from Device Global Memory to Host Local Memory
    OCL_CHECK(err, err = q.enqueueMigrateMemObjects({buffer_out},CL_MIGRATE_MEM_OBJECT_HOST));
    q.finish();

    auto end = chrono::high_resolution_clock::now();
    double duration = (chrono::duration_cast<chrono::microseconds>(end-start).count()) / 1000.0;
    double duration_kernel = (chrono::duration_cast<chrono::microseconds>(t_after_kernel-t_before_kernel).count()) / 1000.0;

    printf("Duration (including memcpy out): %.2lf ms\n", duration); 
    printf("Duration (kernel): %.2lf ms\n", duration_kernel); 

    if (out[0] == sw_num_results) {
        cout << endl << "Result correct!" << endl;
    } else {
        cout << endl << "Result wrong!" << endl;
    }
    cout << "Parsed intersect count : " << sw_num_results << endl;
    cout << "FPGA computed intersect count : " << out[0] << endl;

    int total_task_count = 0;
    for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {
        int task_count = out[1 + PE_id];
	total_task_count += task_count;
        cout << "PE " << PE_id << " computes " << task_count << " page joins" << endl;
    }
        cout << "In total, all PEs compute " << total_task_count << " page joins" << endl;
    
    return  0;
}
