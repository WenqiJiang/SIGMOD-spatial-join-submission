#pragma once

#include "types.hpp"

void parse_page(
    // input
	const int max_entry_num, // set during indexing
    hls::stream<ap_uint<512> > &s_page_A_raw,
    hls::stream<ap_uint<512> > &s_page_B_raw,
    hls::stream<int>& s_join_finish_in,
	// output
    hls::stream<node_meta_t>& s_meta_A,
    hls::stream<node_meta_t>& s_meta_B,
    hls::stream<obj_t>& s_page_A,
    hls::stream<obj_t>& s_page_B,
    hls::stream<int>& s_join_finish_out) {

	const int entry_axi = max_entry_num % 3 == 0? max_entry_num / 3 : max_entry_num / 3 + 1;
	while (true) {

        if (!s_page_A_raw.empty() || !s_page_B_raw.empty()) {

            // read meta data to get the page
            node_meta_t meta_A = parse_meta_data(s_page_A_raw.read());
            node_meta_t meta_B = parse_meta_data(s_page_B_raw.read());
			s_meta_A.write(meta_A);
			s_meta_B.write(meta_B);
    
            // load the two pages 
            LOAD_PAGE:
			// always read the all content up to the max entry size
            for (int i = 0; i < entry_axi; i++) {
#pragma HLS pipeline II=3

                ap_uint<512> reg_A = s_page_A_raw.read();
                ap_uint<512> reg_B = s_page_B_raw.read();

                for (int j = 0; j < N_OBJ_PER_AXI; j++) {

                    // page A
                    ap_uint<32> id_A_ap_uint_32 = reg_A.range(
                        j * OBJ_BITS + 32 * 1 - 1, j * OBJ_BITS + 32 * 0);
                    ap_uint<32> low0_A_ap_uint_32 = reg_A.range(
                        j * OBJ_BITS + 32 * 2 - 1, j * OBJ_BITS + 32 * 1);
                    ap_uint<32> high0_A_ap_uint_32 = reg_A.range(
                        j * OBJ_BITS + 32 * 3 - 1, j * OBJ_BITS + 32 * 2);
                    ap_uint<32> low1_A_ap_uint_32 = reg_A.range(
                        j * OBJ_BITS + 32 * 4 - 1, j * OBJ_BITS + 32 * 3);
                    ap_uint<32> high1_A_ap_uint_32 = reg_A.range(
                        j * OBJ_BITS + 32 * 5 - 1, j * OBJ_BITS + 32 * 4);

                    obj_t obj_A;
                    obj_A.id = *((int*) (&id_A_ap_uint_32));
                    obj_A.low0 = *((float*) (&low0_A_ap_uint_32)); 
                    obj_A.high0 = *((float*) (&high0_A_ap_uint_32)); 
                    obj_A.low1 = *((float*) (&low1_A_ap_uint_32)); 
                    obj_A.high1 = *((float*) (&high1_A_ap_uint_32)); 
                    if (i * N_OBJ_PER_AXI + j < meta_A.count) {
                        s_page_A.write(obj_A);
                    }

                    // Page B
                    ap_uint<32> id_B_ap_uint_32 = reg_B.range(
                        j * OBJ_BITS + 32 * 1 - 1, j * OBJ_BITS + 32 * 0);
                    ap_uint<32> low0_B_ap_uint_32 = reg_B.range(
                        j * OBJ_BITS + 32 * 2 - 1, j * OBJ_BITS + 32 * 1);
                    ap_uint<32> high0_B_ap_uint_32 = reg_B.range(
                        j * OBJ_BITS + 32 * 3 - 1, j * OBJ_BITS + 32 * 2);
                    ap_uint<32> low1_B_ap_uint_32 = reg_B.range(
                        j * OBJ_BITS + 32 * 4 - 1, j * OBJ_BITS + 32 * 3);
                    ap_uint<32> high1_B_ap_uint_32 = reg_B.range(
                        j * OBJ_BITS + 32 * 5 - 1, j * OBJ_BITS + 32 * 4);

                    obj_t obj_B;
                    obj_B.id = *((int*) (&id_B_ap_uint_32));
                    obj_B.low0 = *((float*) (&low0_B_ap_uint_32)); 
                    obj_B.high0 = *((float*) (&high0_B_ap_uint_32)); 
                    obj_B.low1 = *((float*) (&low1_B_ap_uint_32)); 
                    obj_B.high1 = *((float*) (&high1_B_ap_uint_32)); 
                    if (i * N_OBJ_PER_AXI + j < meta_B.count) {
                        s_page_B.write(obj_B);
                    }
                }
			}
		}
        else if (!s_join_finish_in.empty()) {
            int end = s_join_finish_in.read();
            s_join_finish_out.write(end);
            break;
        }  
	}
}

// join between node pairs
// if both the nodes are leaf, write to the results
// if at least on of the nodes is directory, write to layer cache 
void join_page(
    // input
    hls::stream<node_meta_t>& s_meta_A,
    hls::stream<node_meta_t>& s_meta_B,
    hls::stream<obj_t>& s_page_A,
    hls::stream<obj_t>& s_page_B,
    hls::stream<int>& s_join_finish_in,
    // output
    //   for directory nodes: 
    hls::stream<int>& s_intersect_count_directory, // per page pair
    hls::stream<result_t>& s_result_pair_directory,
    //   for leaf nodes: 
    hls::stream<int>& s_intersect_count_leaf, // per page pair
    hls::stream<result_t>& s_result_pair_leaf,
    hls::stream<int>& s_join_finish_out
    ) {

    obj_t page_A[MAX_PAGE_ENTRY_NUM];
    obj_t page_B[MAX_PAGE_ENTRY_NUM];

    // WENQI: as an HLS defect, somehow this loop cannot be compiled successfully using while
    //   so I have to use a counter that counts to (almost infinite) to solve the problem
    // for (long infinite_counter = 0; infinite_counter < 1000 * 1000 * 1000 * 1000; infinite_counter++) {
    while (true) {

        if (!s_meta_A.empty() || !s_meta_B.empty()) {

            node_meta_t meta_A = s_meta_A.read();
            node_meta_t meta_B = s_meta_B.read();
            int max_page_entries = meta_A.count >= meta_B.count? meta_A.count : meta_B.count;
    
            // load the two pages 
            LOAD_PAGE:
            for (int j = 0; j < max_page_entries; j++) {
#pragma HLS pipeline II=1
                if (j < meta_A.count) {
                    page_A[j] = s_page_A.read();
                }
                if (j < meta_B.count) {
                    page_B[j] = s_page_B.read();
                }
            }

            // perform the join 
            // for the case where both trees are data nodes or directory nodes, join directly
            if ((meta_A.is_leaf && meta_B.is_leaf) || (!meta_A.is_leaf && !meta_B.is_leaf)) {

                int intersect_count = 0;
                LOOP_A:
                for (int m = 0; m < meta_A.count; m++) {

                    obj_t obj_A = page_A[m];

                    LOOP_B:
                    for (int n = 0; n < meta_B.count; n++) {
#pragma HLS pipeline II=1

                        obj_t obj_B = page_B[n];

#if POINT_INTERSECT_COUNTS
                        // point overlap is regarded as overlap
                        bool overlap = 
                            // horizontal overlap 
                            (obj_A.high0 >= obj_B.low0) && (obj_B.high0 >= obj_A.low0) && 
                            // vertical no overlap 
                            (obj_A.high1 >= obj_B.low1) && (obj_B.high1 >= obj_A.low1);
#else
                        // point overlap is NOT regarded as overlap, only region overlap counts
                        bool overlap = 
                            // horizontal overlap 
                            (obj_A.high0 > obj_B.low0) && (obj_B.high0 > obj_A.low0) && 
                            // vertical no overlap 
                            (obj_A.high1 > obj_B.low1) && (obj_B.high1 > obj_A.low1);
#endif

                        if (overlap) {
                            intersect_count++;
                            result_t result;
                            result.pair.id_A = obj_A.id;
                            result.pair.id_B = obj_B.id;
                            result.last = false;
                            if (meta_A.is_leaf && meta_B.is_leaf) {
                                s_result_pair_leaf.write(result);
                            } else {
                                s_result_pair_directory.write(result);
                            }
                        }
                    }
                }
                // write last signal
                result_t result;
                result.last = true;
                if (meta_A.is_leaf && meta_B.is_leaf) {
                    s_result_pair_leaf.write(result);
                } 
                // write a dummy last to cache manager even if both are leaves
                s_result_pair_directory.write(result);

                // write count
                if (meta_A.is_leaf && meta_B.is_leaf) {
                    s_intersect_count_leaf.write(intersect_count);
                     // write a dummy count to cache manager even if both are leaves
                    s_intersect_count_directory.write(0);
                } else {
                    s_intersect_count_directory.write(intersect_count);
                }
            } 
            // for the case where one is data node while the other is directory nodes, 
            //    write pairs of data nodes's pointer with diretory nodes' children
            else {
                obj_t leaf_obj;
                obj_t* dir_ptr;
                int count;

                if (meta_A.is_leaf) {
                    leaf_obj = meta_A.obj;
                    dir_ptr = page_B;
                    count = meta_B.count;
                } else {
                    leaf_obj = meta_B.obj;
                    dir_ptr = page_A;
                    count = meta_A.count;
                }

                int intersect_count = 0;
                LOOP_C:
                for (int n = 0; n < count; n++) {
#pragma HLS pipeline II=1

                    obj_t dir_obj = dir_ptr[n];

#if POINT_INTERSECT_COUNTS
                    // point overlap is regarded as overlap
                    bool overlap = 
                        // horizontal overlap 
                        (leaf_obj.high0 >= dir_obj.low0) && (dir_obj.high0 >= leaf_obj.low0) && 
                        // vertical no overlap 
                        (leaf_obj.high1 >= dir_obj.low1) && (dir_obj.high1 >= leaf_obj.low1);
#else
                    // point overlap is NOT regarded as overlap, only region overlap counts
                    bool overlap = 
                        // horizontal overlap 
                        (leaf_obj.high0 > dir_obj.low0) && (dir_obj.high0 > leaf_obj.low0) && 
                        // vertical no overlap 
                        (leaf_obj.high1 > dir_obj.low1) && (dir_obj.high1 > leaf_obj.low1);
#endif

                    if (overlap) {
                        intersect_count++;
                        result_t result;
                        result.pair.id_A = leaf_obj.id;
                        result.pair.id_B = dir_obj.id;
                        result.last = false;
                        s_result_pair_directory.write(result);
                    }
                }
                // write last signal
                result_t result;
                result.last = true;
                s_result_pair_directory.write(result);
                // write count
                s_intersect_count_directory.write(intersect_count);
            }
        } 
        
        else if (!s_join_finish_in.empty()) {
            int end = s_join_finish_in.read();
            s_join_finish_out.write(end);
            break;
        }  
    }
}