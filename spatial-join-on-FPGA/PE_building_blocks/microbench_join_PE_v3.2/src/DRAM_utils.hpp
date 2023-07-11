#pragma once

#include "types.hpp"

// Given an input 64-byte representation, parse it as node meta data type
inline node_meta_t parse_meta_data(ap_uint<512> in_uint512) {

    node_meta_t meta_data;

    ap_uint<32> reg_is_leaf_uint32 = in_uint512.range(32 * 0 + 31, 32 * 0);
    meta_data.is_leaf = *((int*) (&reg_is_leaf_uint32));

    ap_uint<32> reg_count_uint32 = in_uint512.range(32 * 1 + 31, 32 * 1);
    meta_data.count = *((int*) (&reg_count_uint32));

    ap_uint<32> reg_obj_id_uint32 = in_uint512.range(32 * 2 + 31, 32 * 2);
    meta_data.obj.id = *((int*) (&reg_obj_id_uint32));

    ap_uint<32> reg_obj_low0_uint32 = in_uint512.range(32 * 3 + 31, 32 * 3);
    meta_data.obj.low0 = *((float*) (&reg_obj_low0_uint32));

    ap_uint<32> reg_obj_high0_uint32 = in_uint512.range(32 * 4 + 31, 32 * 4);
    meta_data.obj.high0 = *((float*) (&reg_obj_high0_uint32));

    ap_uint<32> reg_obj_low1_uint32 = in_uint512.range(32 * 5 + 31, 32 * 5);
    meta_data.obj.low1 = *((float*) (&reg_obj_low1_uint32));

    ap_uint<32> reg_obj_high1_uint32 = in_uint512.range(32 * 6 + 31, 32 * 6);
    meta_data.obj.high1 = *((float*) (&reg_obj_high1_uint32));

    return meta_data;
}

// send pairs of pages to the compute PE(s)
//   page_num_A * page_num_B pairs in total
// Note: the current design underutilizes the bandwidth for convenience
void read_nodes(
    // input
    int pair_num,
    const int page_bytes, // typically fixed as 4096
	const int max_entry_num, // set during indexing
    const ap_uint<512>* in_pages_A,
    const ap_uint<512>* in_pages_B,
    // output
    hls::stream<ap_uint<512> >& s_page_A_raw,
    hls::stream<ap_uint<512> >& s_page_B_raw,
    hls::stream<int>& s_join_finish_out
    ) {

    const int page_size_per_axi = page_bytes / 64; 
	const int entry_axi = max_entry_num % 3 == 0? max_entry_num / 3 : max_entry_num / 3 + 1;
	const int axi_per_page = 1 + entry_axi;   // number of 64-byte read per node, <= page_bytes, decided by entry_num

    for (int page_id = 0; page_id < pair_num; page_id++) {

		int start_addr_A = page_size_per_axi * page_id;
		int start_addr_B = page_size_per_axi * page_id;

		for (int i = 0; i < axi_per_page; i++) {
#pragma HLS pipeline II=1
			s_page_A_raw.write(in_pages_A[start_addr_A + i]);
			s_page_B_raw.write(in_pages_B[start_addr_B + i]);
		}
	}
	s_join_finish_out.write(1);
}


// slow in writing, no burst inferred
void write_results(

    hls::stream<int>& s_intersect_count_directory, // per page pair
    hls::stream<result_t>& s_result_pair_directory,
    //   for leaf nodes: 
    hls::stream<int>& s_intersect_count_leaf, // per page pair
    hls::stream<result_t>& s_result_pair_leaf,
	
    hls::stream<int>& s_join_finish,
    // output
    ap_uint<64>* out_intersect) {

    ap_uint<64> total_intersect_count = 0;
    const int bias = 1; // the first number writes total intersection count, 
	result_t tmp_result;

    while (true) {
		if (!s_intersect_count_directory.empty()) {
			int c = s_intersect_count_directory.read();
			for (int i = 0; i < c; i++) {
#pragma HLS pipeline II=1
				tmp_result = s_result_pair_directory.read();
			}
		} else if (!s_intersect_count_leaf.empty()) {
			int c = s_intersect_count_leaf.read();
			for (int i = 0; i < c; i++) {
#pragma HLS pipeline II=1
				tmp_result = s_result_pair_leaf.read();
			}
		} else if (!s_join_finish.empty() && s_result_pair_directory.empty() && s_result_pair_leaf.empty()) {
			int finish = s_join_finish.read();
			break;
		}
	}

    // write the number of intersection in the first address
    out_intersect[0] = total_intersect_count;
	s_join_finish.read();
}