#pragma once

#include "types.hpp"
#include "utils.hpp"
#include "hls_burst_maxi.h"

// Given an input 64-byte representation, parse it as node meta data type
node_meta_t parse_meta_data(ap_uint<512> in_uint512) {
#pragma HLS inline

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

// Input: the read request (pair of page IDs) from the scheduler
// Output: the meta data and entries of the nodes of the pair of nodes
// Note: the current design underutilizes the bandwidth for convenience
void read_nodes(
    // input
    const int page_bytes, // typically fixed as 4096
	const int max_entry_num, // set during indexing
    hls::burst_maxi<ap_uint<512> > in_pages_A,
    hls::burst_maxi<ap_uint<512> > in_pages_B,
    hls::stream<pair_t>& axis_page_ID_pair_read_nodes,
    hls::stream<int>& axis_join_PE_ID,
    hls::stream<int>& s_join_finish_in,
    // output
    hls::stream<ap_uint<512> > (&s_page_A_raw)[N_JOIN_PE],
    hls::stream<ap_uint<512> > (&s_page_B_raw)[N_JOIN_PE],
    hls::stream<int>& s_join_finish_out
    ) {

    const int page_size_per_axi = page_bytes / 64; 
	const int entry_axi = max_entry_num % 3 == 0? max_entry_num / 3 : max_entry_num / 3 + 1;
	const int axi_per_page = 1 + entry_axi;   // number of 64-byte read per node, <= page_bytes, decided by entry_num

	// Note: here, must make sure the total read length request does not exceed num_read_outstanding * max_read_burst_length, otherwise
	//   deadlock will appear, because the data FIFO is full
	const int max_prefetch_request = 16;
	int join_PE_ID_cache[max_prefetch_request];

    while (true) {

        if (!axis_page_ID_pair_read_nodes.empty() || !axis_join_PE_ID.empty()) {

			//    1. send prefetch request (if not enough outstanding requests are in the pipeline)
			//    2. reading data
			int current_request_count = 0;

			while ((!axis_page_ID_pair_read_nodes.empty() || !axis_join_PE_ID.empty()) &&
				current_request_count < max_prefetch_request ) {

				pair_t page_ID_pair = block_read<pair_t>(axis_page_ID_pair_read_nodes);

				int page_ID_A = page_ID_pair.id_A;
				int page_ID_B = page_ID_pair.id_B;
				int start_addr_A = page_size_per_axi * page_ID_A;
				int start_addr_B = page_size_per_axi * page_ID_B;
				
				in_pages_A.read_request(start_addr_A, axi_per_page);
				in_pages_B.read_request(start_addr_B, axi_per_page);
				
				join_PE_ID_cache[current_request_count] = block_read<int>(axis_join_PE_ID);
				current_request_count++;
			}

			for (int j = 0; j < current_request_count; j++) {
				// always read the all content up to the max entry size
				int join_PE_ID = join_PE_ID_cache[j];
				for (int i = 0; i < axi_per_page; i++) {
#pragma HLS pipeline II=1
					s_page_A_raw[join_PE_ID].write(in_pages_A.read());
					s_page_B_raw[join_PE_ID].write(in_pages_B.read());
                }
			}
        } else if (!s_join_finish_in.empty()) {
            int end = s_join_finish_in.read();
            s_join_finish_out.write(end);
            break;
        }  
    }
}

// burst buffer removes the last signal and replace it by a special last
void burst_buffer(
    // input
    hls::stream<result_t>& s_result_pair, 
    hls::stream<int>& s_join_finish_in, 

    // output
    hls::stream<int>& s_result_pair_burst_length, 
    hls::stream<int>& s_result_pair_burst_contain_last, 
    hls::stream<result_t>& s_result_pair_burst, 
    hls::stream<int>& s_join_finish_out
) {

    // max_burst_length must <= the output FIFO length & AXI burst length
    const int max_burst_length = 512;

    while (true) {

        if (!s_result_pair.empty()) {

            int burst_length = max_burst_length;

            // send data 
            for (int i = 0; i < max_burst_length; i++) {
#pragma HLS pipeline II=1

                result_t result = s_result_pair.read();

                // if last, the join PE will not produce new result immediately,
                //    as it has to load the next pair of pages
                if (result.last) {
                    burst_length = i; // last do not count
                    break;
                } else {
					// only write non-last content
					s_result_pair_burst.write(result);
				}
            }

            // send data length, length is always > 0 because we did data empty check
            s_result_pair_burst_length.write(burst_length);
			if (burst_length < max_burst_length) {
				s_result_pair_burst_contain_last.write(1);
			} else {
				s_result_pair_burst_contain_last.write(0);
			}
        }
        else if (!s_join_finish_in.empty()) {
            int break_signal = s_join_finish_in.read(); // must read to make dataflow work
            s_join_finish_out.write(break_signal);
            break;
        }
    }
}


void layer_cache_memory_controller(
    // input
    //   argument
    int max_level_A,
    int max_level_B,
    int root_id_A,
    int root_id_B,
    //   memory 
	hls::burst_maxi<ap_uint<64> > layer_cache,
    //   from join PE
    hls::stream<int> (&s_intersect_count_directory)[N_JOIN_PE], 
    hls::stream<int> (&s_result_pair_directory_burst_length)[N_JOIN_PE],
    hls::stream<int> (&s_result_pair_directory_burst_contain_last)[N_JOIN_PE], 
    hls::stream<result_t> (&s_result_pair_directory_burst)[N_JOIN_PE],
    //   from scheduler
    hls::stream<mem_burst_t>& axis_layer_cache_read_info, 
    hls::stream<int>& axis_layer_cache_write_addr, 
    hls::stream<int>& axis_num_layer_pairs, // number of pairs to join in this layer
    hls::stream<int>& s_join_finish_in,
    // output
    //   to scheduler
    hls::stream<pair_t>& axis_page_pair_scheduler,      // for read request, return pair
    hls::stream<int>& axis_intersect_count_directory_scheduler, // for write request, return count
    hls::stream<int>& s_join_finish_out
) {

    int max_level = max_level_A > max_level_B ? max_level_A: max_level_B;
    
    // Initialization: write the pair (rootA, rootB) in layer cache 0
    pair_t root_pair;
    root_pair.id_A = root_id_A;
    root_pair.id_B = root_id_B;
    ap_uint<64> root_ap_uint_64 = pack_pair(root_pair);

	layer_cache.write_request(0, 1);
    layer_cache.write(root_ap_uint_64); 
	layer_cache.write_response();

	// Manual burst: https://docs.xilinx.com/r/2022.1-English/ug1399-vitis-hls/Using-Manual-Burst
	const int max_write_requests = 32;
	int join_PE_ID_cache[max_write_requests];
	int burst_length_cache[max_write_requests];

    // Order:
    //   1. For each layer, read the write control signal, the write address, and the total
    //        number of pairs in the layer
    //   2. Loop over the join PEs to collect results; check read cache request regularly; 
    //        end the loop if the number of last signal by join PE == number of pairs in the layer

    // non-leaf join, write to the layer cache 
    for (int current_level = 1; current_level < max_level + 1; current_level++) {

        // number of pairs to process in the layer
        int num_layer_pairs = block_read<int>(axis_num_layer_pairs);

        // write signal and address
        int write_start_addr = block_read<int>(axis_layer_cache_write_addr);
        int write_count = 0;
        int last_count = 0; // number of page joins finished
        bool break_while = false;

        while (!break_while) {

            // read signal arrives
            if (!axis_layer_cache_read_info.empty()) { 
                mem_burst_t read_info = block_read<mem_burst_t>(axis_layer_cache_read_info);
				layer_cache.read_request(read_info.addr, read_info.num);

                for (int i = 0; i < read_info.num; i++) {
#pragma HLS pipeline II=1
                    pair_t next_page_pair = unpack_pair(layer_cache.read());
                    axis_page_pair_scheduler.write(next_page_pair);
                }
            } 

            // write to memory by loading content from join PEs
            else { 

				int current_request_count = 0;
				int total_burst_length = 0; // gather up to 32 write requests

                // chech all PEs in a round-robin manner
                for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {

                    while (!s_result_pair_directory_burst_length[PE_id].empty() && current_request_count < max_write_requests) {

                        int burst_length = s_result_pair_directory_burst_length[PE_id].read();

						if (burst_length > 0) {
							join_PE_ID_cache[current_request_count] = PE_id;
							burst_length_cache[current_request_count] = burst_length;
							total_burst_length += burst_length; 
							current_request_count++;
						}
						int contain_last = s_result_pair_directory_burst_contain_last[PE_id].read();
						if (contain_last) {
							int tmp_count = s_intersect_count_directory[PE_id].read(); 
							last_count++;
							if (last_count == num_layer_pairs) {
								break_while = true; 
							}
						}
                    }
                }

				// write all these bursts
				if (total_burst_length > 0) {
					layer_cache.write_request(write_start_addr + write_count, total_burst_length);
					for (int bid = 0; bid < current_request_count; bid++) {
						int burst_length = burst_length_cache[bid];
						int PE_id = join_PE_ID_cache[bid];
						for (int i = 0; i < burst_length; i++) {
#pragma HLS pipeline II=1
							result_t result = s_result_pair_directory_burst[PE_id].read();
							ap_uint<64> result_ap_uint_64 = pack_pair(result.pair);
							layer_cache.write(result_ap_uint_64);
						}
					}
					write_count += total_burst_length;
					layer_cache.write_response();
				}
            }
        }

        // write the total number of pairs in this layer
        if (current_level < max_level) {    
            axis_intersect_count_directory_scheduler.write(write_count);
        }
    }

    int end = s_join_finish_in.read();
    s_join_finish_out.write(end);
}


void write_results(
    // input
    //   from join PE
    hls::stream<int> (&s_intersect_count_leaf)[N_JOIN_PE], // per page pair
    hls::stream<int> (&s_result_pair_leaf_burst_length)[N_JOIN_PE],
    hls::stream<int> (&s_result_pair_leaf_burst_contain_last)[N_JOIN_PE], 
    hls::stream<result_t> (&s_result_pair_leaf_burst)[N_JOIN_PE],
    //   from the scheduler
    hls::stream<int>& axis_PE_task_count,
    hls::stream<int>& s_join_finish_in,  // final end signal 
    // out
    //    out format: the first number writes total intersection count, 
    //                while the rest are intersect ID pairs
    hls::burst_maxi<ap_uint<64> > out_intersect) {

    ap_uint<64> total_intersect_count = 0; 
    const int bias = 1 + N_JOIN_PE; // the first number writes total intersection count, the rest write tasks per PE

	// Manual burst: https://docs.xilinx.com/r/2022.1-English/ug1399-vitis-hls/Using-Manual-Burst
	const int max_write_requests = 32;
	int join_PE_ID_cache[max_write_requests];
	int burst_length_cache[max_write_requests];

    while (true) {

        // whether any PE has results in this round, used for finish detection
        bool has_content = false; 
		int current_request_count = 0;
		int total_burst_length = 0; // gather up to 32 write requests

        // loop over join PEs to read results
        for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {

            // if there's more than 1 result in the FIFO, keep reading it
            while (!s_result_pair_leaf_burst_length[PE_id].empty() && current_request_count < max_write_requests) {

                has_content = true; 

                int burst_length = s_result_pair_leaf_burst_length[PE_id].read();

				if (burst_length > 0) {
					join_PE_ID_cache[current_request_count] = PE_id;
					burst_length_cache[current_request_count] = burst_length;
					total_burst_length += burst_length; 
					current_request_count++;
				}

				int contain_last = s_result_pair_leaf_burst_contain_last[PE_id].read();
                if (contain_last) {
                    int tmp_count = s_intersect_count_leaf[PE_id].read(); 
                } 
            }
        }

		// write all these bursts
		if (total_burst_length > 0) {
			out_intersect.write_request(total_intersect_count + bias, total_burst_length);
			for (int bid = 0; bid < current_request_count; bid++) {
				int burst_length = burst_length_cache[bid];
				int PE_id = join_PE_ID_cache[bid];
				for (int i = 0; i < burst_length; i++) {
#pragma HLS pipeline II=1
					result_t result = s_result_pair_leaf_burst[PE_id].read();
					ap_uint<64> result_ap_uint_64 = pack_pair(result.pair);
					out_intersect.write(result_ap_uint_64);
				}
			}
			total_intersect_count += total_burst_length;
			out_intersect.write_response();
		}

        // check whether the entire join is finished
        if (!has_content && !s_join_finish_in.empty()) {

            bool has_content_recheck = false; // whether any PE has results in this round

            // loop over the FIFOs to double check if there's any content
            for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {
                if (!s_result_pair_leaf_burst[PE_id].empty() || !s_result_pair_leaf_burst_length[PE_id].empty()) { 
                    has_content_recheck = true; 
                    // Wenqi: no break here, all the empty signals will be and in a single cycle,
                    //   and the break will break the outer while loop instead of this for loop 
                    //   (HLS behavior problem)
                    ////// break;
                }
            }
            if (!has_content_recheck) {
                int end = s_join_finish_in.read();
                break;
            }
        }
    }

	out_intersect.write_request(0, 1 + N_JOIN_PE);

    out_intersect.write(total_intersect_count); // out_intersect[0]
    // write the per PE task count, as ap_uint<64>
    for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {
        int task_count = block_read<int>(axis_PE_task_count);
        out_intersect.write(task_count);
    }
	out_intersect.write_response();
}
