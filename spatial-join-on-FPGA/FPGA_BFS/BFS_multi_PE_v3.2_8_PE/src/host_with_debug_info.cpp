#include <stdint.h>
#include <chrono>
#include <regex> 

#include "host.hpp"

#include "constants.hpp"
// Wenqi: seems 2022.1 somehow does not support linking ap_uint.h to host?
// #include "ap_uint.h"

#define LOAD_INDEX // load binary tree index
// #define MANUAL_INDEX // manually malloc and write contents

using namespace std;

// boost::filesystem does not compile well, so implement this myself
string dir_concat(string dir1, string dir2) {
    if (dir1.back() != '/') {
        dir1 += '/';
    }
    return dir1 + dir2;
}

int main(int argc, char** argv)
{
    cl_int err;
    // Allocate Memory in Host Memory
    // When creating a buffer with user pointer (CL_MEM_USE_HOST_PTR), under the hood user ptr 
    // is used if it is properly aligned. when not aligned, runtime had no choice but to create
    // its own host side buffer. So it is recommended to use this allocator if user wish to
    // create buffer using CL_MEM_USE_HOST_PTR to align user buffer to page boundary. It will 
    // ensure that user buffer is used when user create Buffer/Mem object with CL_MEM_USE_HOST_PTR 


#ifdef LOAD_INDEX

    int max_level_A;
    int max_level_B;
    int root_id_A = 0;
    int root_id_B = 0;
    int page_bytes = 4096;

    string tree_parent_dir = "/mnt/scratch/wenqi/spatial-join-on-FPGA/tree_bin";
    // string tree_fname = "sample_tree_level_1_self_join_156.bin";
    // string tree_fname = "sample_tree_level_2_self_join_2090.bin";
    // string tree_fname = "sample_tree_level_3_self_join_19246.bin";
    string tree_fname = "sample_tree_level_4_self_join_235112.bin";
    string tree_bin_dir = dir_concat(tree_parent_dir, tree_fname);

    // regex in c++: https://www.softwaretestinghelp.com/regex-in-cpp/
    regex reg_level("level_[0-9]+");
    regex reg_intersects("self_join_[0-9]+");

    smatch level_match; 
    smatch intersects_match; 
   
    // regex_search that searches pattern regexp in the string mystr  
    regex_search(tree_fname, level_match, reg_level);
    regex_search(tree_fname, intersects_match, reg_intersects);

    string level_str; // = level_match.begin().str().substr(6);
    string intersect_str; // = intersects_match.begin().str().substr(10);
    for (auto x : level_match) level_str = x.str().substr(6);
    for (auto x : intersects_match) intersect_str = x.str().substr(10);

    max_level_A = stol(level_str);
    max_level_B = stol(level_str);

    long correct_intersect = stol(intersect_str);

    cout << "Parsed tree level A : " << max_level_A << endl;
    cout << "Parsed tree level B : " << max_level_B << endl;
    cout << "Parsed intersect counts : " << correct_intersect << endl;

    // get data size from disk 
    ifstream tree_fstream(
        tree_bin_dir, ios::in | ios::binary);
    tree_fstream.seekg(0, tree_fstream.end);
    int64_t tree_bytes = tree_fstream.tellg();
    tree_fstream.seekg(0, tree_fstream.beg);
    cout << "Tree bytes: " << tree_bytes << endl;

    // allocate memory 
    cout << "Allocating memory...\n";
    size_t bytes_page_A = tree_bytes;
    size_t bytes_page_B = tree_bytes;
    vector<int ,aligned_allocator<int>> in_pages_A(bytes_page_A / sizeof(int), 0);
    vector<int ,aligned_allocator<int>> in_pages_B(bytes_page_B / sizeof(int), 0);

    // size_t out_bytes = 10 * 1024 * 1024;
    size_t layer_cache_bytes = 1 * size_t(1000) * size_t(1000) * size_t(1000); // no more than 16 GB
    cout << "layer_cache_bytes: " << layer_cache_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> layer_cache(layer_cache_bytes / sizeof(int64_t), 0);

    // size_t out_bytes = 10 * 1024 * 1024;
    size_t out_bytes = 1 * size_t(1000) * size_t(1000) * size_t(1000); // no more than 16 GB
    cout << "out_bytes: " << out_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> out(out_bytes / sizeof(int64_t), 0);

    // load data from disk
    tree_fstream.read((char*) in_pages_A.data(), tree_bytes);
    if (!tree_fstream) {
            cout << "error: only " << tree_fstream.gcount() << " could be read";
        exit(1);
    }
    memcpy(in_pages_B.data(), in_pages_A.data(), tree_bytes);

    cout << "Root A info: " << endl;
    cout << "  is leaf: " << in_pages_A[0] << endl;
    cout << "  count: " << in_pages_A[1] << endl;

    cout << "Root B info: " << endl;
    cout << "  is leaf: " << in_pages_B[0] << endl;
    cout << "  count: " << in_pages_B[1] << endl;

    // cout << "First obj in A: " << endl;
    // cout << " ID: " << in_pages_A[16 + 0] << endl;
    // float tmp_A_bound_low0 = *((float*) (&in_pages_A[16 + 1]));
    // float tmp_A_bound_high0 = *((float*) (&in_pages_A[16 + 2]));
    // float tmp_A_bound_low1 = *((float*) (&in_pages_A[16 + 3]));
    // float tmp_A_bound_high1 = *((float*) (&in_pages_A[16 + 4]));
    // cout << " Boundary: low 0: " << tmp_A_bound_low0 << 
    //     " high 0: " << tmp_A_bound_high0 << 
    //     " low 1: " << tmp_A_bound_low1 <<
    //     " high 1: " << tmp_A_bound_high1 << endl;

    // cout << "First obj in B: " << endl;
    // cout << " ID: " << in_pages_B[16 + 0] << endl;
    // float tmp_B_bound_low0 = *((float*) (&in_pages_B[16 + 1]));
    // float tmp_B_bound_high0 = *((float*) (&in_pages_B[16 + 2]));
    // float tmp_B_bound_low1 = *((float*) (&in_pages_B[16 + 3]));
    // float tmp_B_bound_high1 = *((float*) (&in_pages_B[16 + 4]));
    // cout << " Boundary: low 0: " << tmp_B_bound_low0 << 
    //     " high 0: " << tmp_B_bound_high0 << 
    //     " low 1: " << tmp_B_bound_low1 <<
    //     " high 1: " << tmp_B_bound_high1 << endl;

    // int root_count_A = in_pages_A[1];
    // int root_count_B = in_pages_B[1];
    // vector<float> objects_A(4 * root_count_A, 0);
    // vector<float> objects_B(4 * root_count_B, 0);

    // // copy contents to a dense array
    // for (int i = 0; i < root_count_A / 3 + 1; i++) {
    //     for (int j = 0; j < 3; j++) {
    //         if (i * 3 + j >= root_count_A) {
    //             break;
    //         }
    //         int start_addr = (64 + i * 64 + j * 20 + 4) / sizeof(int);
    //         int dense_addr = (i * 3 + j) * 4;
    //         objects_A[dense_addr + 0] = *((float*) (&in_pages_A[start_addr + 0]));
    //         objects_A[dense_addr + 1] = *((float*) (&in_pages_A[start_addr + 1]));
    //         objects_A[dense_addr + 2] = *((float*) (&in_pages_A[start_addr + 2]));
    //         objects_A[dense_addr + 3] = *((float*) (&in_pages_A[start_addr + 3]));
    //         cout << "Root A, obj " << i * 3 + j << " " << objects_A[dense_addr + 0] << 
    //             " " << objects_A[dense_addr + 1] << 
    //             " " << objects_A[dense_addr + 2] << 
    //             " " << objects_A[dense_addr + 3] << endl;
    //     }
    // }
    // for (int i = 0; i < root_count_B / 3 + 1; i++) {
    //     for (int j = 0; j < 3; j++) {
    //         if (i * 3 + j >= root_count_B) {
    //             break;
    //         }
    //         int start_addr = (64 + i * 64 + j * 20 + 4) / sizeof(int);
    //         int dense_addr = (i * 3 + j) * 4;
    //         objects_B[dense_addr + 0] = *((float*) (&in_pages_B[start_addr + 0]));
    //         objects_B[dense_addr + 1] = *((float*) (&in_pages_B[start_addr + 1]));
    //         objects_B[dense_addr + 2] = *((float*) (&in_pages_B[start_addr + 2]));
    //         objects_B[dense_addr + 3] = *((float*) (&in_pages_B[start_addr + 3]));
    //         cout << "Root B, obj " << i * 3 + j << " " << objects_B[dense_addr + 0] << 
    //             " " << objects_B[dense_addr + 1] << 
    //             " " << objects_B[dense_addr + 2] << 
    //             " " << objects_B[dense_addr + 3] << endl;
    //     }
    // }

    // // compute sw intersect
    // int sw_intersect_count = 0;
    // for (int i = 0; i < root_count_A; i++) {
    //     for (int j = 0; j < root_count_B; j++) {
    //         float low0_A = objects_A[4 * i + 0];
    //         float high0_A = objects_A[4 * i + 1];
    //         float low1_A = objects_A[4 * i + 2];
    //         float high1_A = objects_A[4 * i + 3];

    //         float low0_B = objects_B[4 * j + 0];
    //         float high0_B = objects_B[4 * j + 1];
    //         float low1_B = objects_B[4 * j + 2];
    //         float high1_B = objects_B[4 * j + 3];

    //         if ((high0_A >= low0_B) && (high0_B >= low0_A) && 
    //             (high1_A >= low1_B) && (high1_B >= low1_A)) {
    //             sw_intersect_count++;
    //         }
    //     }
    // }
    // cout << "sw intersect count: " << sw_intersect_count << endl;

#endif 

#ifdef MANUAL_INDEX
    int max_level_A = 2;
    int max_level_B = 2;
    int root_id_A = 0;
    int root_id_B = 0;
    int page_bytes = 4096;

    // in init
    int root_children_num = 32;
    cout << "Root's children number: " << root_children_num << endl;
    size_t page_num_A = 1 + root_children_num; // root and first level children
    size_t page_num_B = 1 + root_children_num; // root and first level children

    size_t bytes_page_A = page_num_A * page_bytes;
    size_t bytes_page_B = page_num_B * page_bytes;

    cout << "bytes per page: " << page_bytes << endl;
    vector<int ,aligned_allocator<int>> in_pages_A(bytes_page_A, 0);
    vector<int ,aligned_allocator<int>> in_pages_B(bytes_page_B, 0);

    // size_t out_bytes = 10 * 1024 * 1024;
    size_t layer_cache_bytes = 1 * size_t(1000) * size_t(1000) * size_t(1000); // no more than 16 GB
    cout << "layer_cache_bytes: " << layer_cache_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> layer_cache(layer_cache_bytes / sizeof(int64_t), 0);

    // size_t out_bytes = 10 * 1024 * 1024;
    size_t out_bytes = 4 * size_t(1000) * size_t(1000) * size_t(1000); // no more than 16 GB
    cout << "out_bytes: " << out_bytes << endl;
    vector<int64_t ,aligned_allocator<int64_t>> out(out_bytes / sizeof(int64_t), 0);

    // set page contents
    // The first 64-bytes per page:
    // typedef struct {
    //     // 7 * 4 bytes = 28 bytes
    //     int is_leaf;  // bool 
    //     int count;    // valid items
    //     obj_t obj;    // id/ptr + mbr
    // } node_meta_t;
    
    // root -> set basic info
    in_pages_A[0] = 0; // is_leaf
    in_pages_A[1] = root_children_num; // valid items
    in_pages_A[2] = 0; // id items

    in_pages_B[0] = 0; // is_leaf
    in_pages_B[1] = root_children_num; // valid items
    in_pages_B[2] = 0; // id items

    // root -> set page contents
    // in_pages_A[16] = 1;
    // in_pages_B[16] = 1;
    const int axi_bytes = 64; // 64 byte 
    const int axi_int = 64 / sizeof(int); // 16 int per page
    const int obj_int = 20 / sizeof(int); // 20 byte (5 int ) per object
    const int header_bias = axi_int; // first 64 byte = header
    for (int i = 0; i < root_children_num / 3 + 1; i++) {
        
        for (int j = 0; j < 3; j++) { // 3 obj per AXI
            // addr for obj id (page id for directory nodes)
            int children_id = i * 3 + j + 1; // root id = 0, children = 1, 2, ...
            if (children_id < 1 + root_children_num) {
                int addr = header_bias + i * axi_int + j * obj_int + 0;
                in_pages_A[addr] = children_id;
                in_pages_B[addr] = children_id;
            }
        }
    }

    // data nodes
    for (int i = 1; i < page_num_A; i++) {
        int bias = i * page_bytes / sizeof(int);
        in_pages_A[bias] = 1; // is_leaf
        in_pages_A[bias + 1] = 128; // valid items
        in_pages_A[bias + 2] = i; // id
    }
    for (int i = 1; i < page_num_B; i++) {
        int bias = i * page_bytes / sizeof(int);
        in_pages_B[bias] = 1; // is_leaf
        in_pages_B[bias + 1] = 128; // valid items
        in_pages_B[bias + 2] = i; // id
    }
#endif 
// OPENCL HOST CODE AREA START

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
    cl::Program program(context, devices, vadd_bins);
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

    OCL_CHECK(err, err = krnl_executor.setArg(0, int(root_id_A)));
    OCL_CHECK(err, err = krnl_executor.setArg(1, int(root_id_B)));
    OCL_CHECK(err, err = krnl_executor.setArg(1, int(page_bytes)));
    OCL_CHECK(err, err = krnl_executor.setArg(8, buffer_in_pages_A));
    OCL_CHECK(err, err = krnl_executor.setArg(9, buffer_in_pages_B));
    OCL_CHECK(err, err = krnl_executor.setArg(10, buffer_layer_cache));
    OCL_CHECK(err, err = krnl_executor.setArg(11, buffer_out));


    // Copy input data to device global memory
    OCL_CHECK(err, err = q.enqueueMigrateMemObjects({
        // in
        buffer_in_pages_A,
        buffer_in_pages_B,
        buffer_layer_cache,
        buffer_out
        },0/* 0 means from host*/));
    q.finish();

    cout << "Launching kernel...\n";
    // Launch the Kernel
    auto start = chrono::high_resolution_clock::now();
    OCL_CHECK(err, err = q.enqueueTask(krnl_scheduler));
    OCL_CHECK(err, err = q.enqueueTask(krnl_executor));
    q.finish();
    
    // Copy Result from Device Global Memory to Host Local Memory
    OCL_CHECK(err, err = q.enqueueMigrateMemObjects({buffer_out},CL_MIGRATE_MEM_OBJECT_HOST));
    q.finish();

    auto end = chrono::high_resolution_clock::now();
    double duration = (chrono::duration_cast<chrono::milliseconds>(end-start).count() / 1000.0);

    cout << "Duration (including memcpy out): " << duration << " sec" << endl; 

    cout << "Intersect pair number: " << out[0] << endl;


#ifdef LOAD_INDEX
    if (out[0] == correct_intersect) {
        cout << endl << "Result correct!" << endl;
    } else {
        cout << endl << "Result wrong!" << endl;
    }
    cout << "Parsed intersect count : " << correct_intersect << endl;
    cout << "FPGA computed intersect count : " << out[0] << endl;
#else
    cout << "TEST FINISHED (NO RESULT CHECK)" << endl; 
#endif 
    return  0;
}
