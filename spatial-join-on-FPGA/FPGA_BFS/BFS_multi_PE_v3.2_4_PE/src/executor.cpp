#include "constants.hpp"
#include "DRAM_utils.hpp"
#include "join_PE.hpp"
#include "types.hpp"

extern "C" {

void executor(  
    ///// input /////
    int max_level_A,
    int max_level_B,
    int root_id_A,
    int root_id_B,
    int page_bytes,
	int max_entry_num, // set during indexing
    // input streams
    //   to layer cache controller
    hls::stream<mem_burst_t>& axis_layer_cache_read_info, 
    hls::stream<int>& axis_layer_cache_write_addr, 
    hls::stream<int>& axis_num_layer_pairs, // number of pairs to join in this layer
    //   to node reading PE
    hls::stream<pair_t>& axis_page_ID_pair_read_nodes,
    hls::stream<int>& axis_join_PE_ID,
    //   to the write results PE
    hls::stream<int>& axis_PE_task_count, 
    hls::stream<int>& axis_join_finish,   
    // input from DRAM
    hls::burst_maxi<ap_uint<512> > in_pages_A,
    hls::burst_maxi<ap_uint<512> > in_pages_B,

    ///// output /////
    // output streams
    //   from layer cache controller
    hls::stream<pair_t>& axis_page_pair_scheduler,      // for read request, return pair
    hls::stream<int>& axis_intersect_count_directory_scheduler, // for write request, return count  
    
    // in/out (intermediate)
    hls::burst_maxi<ap_uint<64> > layer_cache,
    // out (result) format: the first number writes total intersection count, 
    //   while the rest are intersect ID pairs
    hls::burst_maxi<ap_uint<64> > out_intersect 
    )
{
// Share the same AXI interface with several control signals (but they are not allowed in same dataflow)
//    https://docs.xilinx.com/r/en-US/ug1399-vitis-hls/Controlling-AXI4-Burst-Behavior

// input streams
#pragma HLS INTERFACE axis port=axis_layer_cache_read_info
#pragma HLS INTERFACE axis port=axis_layer_cache_write_addr
#pragma HLS INTERFACE axis port=axis_num_layer_pairs
#pragma HLS INTERFACE axis port=axis_page_ID_pair_read_nodes
#pragma HLS INTERFACE axis port=axis_join_PE_ID
#pragma HLS INTERFACE axis port=axis_PE_task_count
#pragma HLS INTERFACE axis port=axis_join_finish

// in runtime (should from DRAM)
#pragma HLS INTERFACE m_axi port=in_pages_A latency=1 num_read_outstanding=32 max_read_burst_length=32  offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=in_pages_B latency=1 num_read_outstanding=32 max_read_burst_length=32  offset=slave bundle=gmem1

// output streams
#pragma HLS INTERFACE axis port=axis_page_pair_scheduler
#pragma HLS INTERFACE axis port=axis_intersect_count_directory_scheduler

// out 
#pragma HLS INTERFACE m_axi port=layer_cache latency=1 num_read_outstanding=32 max_read_burst_length=16 num_write_outstanding=32 max_write_burst_length=16 offset=slave bundle=gmem2  max_write_burst_length=64
#pragma HLS INTERFACE m_axi port=out_intersect latency=1 num_write_outstanding=32 max_write_burst_length=16  offset=slave bundle=gmem3  max_write_burst_length=64

#pragma HLS dataflow

////////////////////     First Half: ADC     ////////////////////


    hls::stream<ap_uint<512> > s_page_A_raw[N_JOIN_PE];
#pragma HLS stream variable=s_page_A_raw depth=512
    hls::stream<ap_uint<512> > s_page_B_raw[N_JOIN_PE];
#pragma HLS stream variable=s_page_B_raw depth=512

    hls::stream<node_meta_t> s_meta_A[N_JOIN_PE];
#pragma HLS stream variable=s_meta_A depth=32
    hls::stream<node_meta_t> s_meta_B[N_JOIN_PE];
#pragma HLS stream variable=s_meta_B depth=32

    hls::stream<obj_t> s_page_A[N_JOIN_PE];
#pragma HLS stream variable=s_page_A depth=512
    hls::stream<obj_t> s_page_B[N_JOIN_PE];
#pragma HLS stream variable=s_page_B depth=512


    hls::stream<int> s_intersect_count_directory[N_JOIN_PE];
#pragma HLS stream variable=s_intersect_count_directory depth=32

    hls::stream<result_t> s_result_pair_directory[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_directory depth=512

    hls::stream<int> s_intersect_count_leaf[N_JOIN_PE]; 
#pragma HLS stream variable=s_intersect_count_leaf depth=32

    hls::stream<result_t> s_result_pair_leaf[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_leaf depth=512


    hls::stream<int> s_result_pair_directory_burst_length[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_directory_burst_length depth=32

    hls::stream<int> s_result_pair_directory_burst_contain_last[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_directory_burst_contain_last depth=32

    hls::stream<result_t> s_result_pair_directory_burst[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_directory_burst depth=512

    hls::stream<int> s_result_pair_leaf_burst_length[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_leaf_burst_length depth=32

    hls::stream<int> s_result_pair_leaf_burst_contain_last[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_leaf_burst_contain_last depth=32

    hls::stream<result_t> s_result_pair_leaf_burst[N_JOIN_PE];
#pragma HLS stream variable=s_result_pair_leaf_burst depth=512



    // finish signals
    hls::stream<int> s_finish_scheduler_out; 
#pragma HLS stream variable=s_finish_scheduler_out depth=2

    hls::stream<int> s_finish_read_out; 
#pragma HLS stream variable=s_finish_read_out depth=2

    hls::stream<int> s_finish_read_out_replicated[N_JOIN_PE]; 
#pragma HLS stream variable=s_finish_read_out_replicated depth=2

    hls::stream<int> s_finish_parse_page_out[N_JOIN_PE]; 
#pragma HLS stream variable=s_finish_parse_page_out depth=2

    hls::stream<int> s_finish_join_PE_out[N_JOIN_PE]; 
#pragma HLS stream variable=s_finish_join_PE_out depth=2

    hls::stream<int> s_finish_layer_cache_burst_out[N_JOIN_PE]; 
#pragma HLS stream variable=s_finish_layer_cache_burst_out depth=2

    hls::stream<int> s_finish_write_burst_out[N_JOIN_PE]; 
#pragma HLS stream variable=s_finish_write_burst_out depth=2

    hls::stream<int> s_finish_join_PE_out_aggregated; 
#pragma HLS stream variable=s_finish_join_PE_out_aggregated depth=2

    hls::stream<int> s_finish_layer_cache_out; 
#pragma HLS stream variable=s_finish_layer_cache_out depth=2


    pass_termination_signal(
        // from AXIS
        axis_join_finish,
        // to internal PEs
        s_finish_scheduler_out
    ); 

	read_nodes(
		// input
		page_bytes, // typically fixed as 4096
		max_entry_num, // set during indexing
		in_pages_A,
		in_pages_B,
		axis_page_ID_pair_read_nodes,
        axis_join_PE_ID,
		s_finish_scheduler_out,
		// output
		s_page_A_raw,
		s_page_B_raw,
		s_finish_read_out
    );

    broadcast_finish_signals<N_JOIN_PE>(
        s_finish_read_out,
        s_finish_read_out_replicated);

    for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {
#pragma HLS unroll
		parse_page(
			// input
			max_entry_num, // set during indexing
			s_page_A_raw[PE_id],
			s_page_B_raw[PE_id],
			s_finish_read_out_replicated[PE_id],
			// output
			s_meta_A[PE_id],
			s_meta_B[PE_id],
			s_page_A[PE_id],
			s_page_B[PE_id],
			s_finish_parse_page_out[PE_id]);

        join_page(
            // input
            s_meta_A[PE_id],
            s_meta_B[PE_id],
            s_page_A[PE_id],
            s_page_B[PE_id],
            s_finish_parse_page_out[PE_id],
            // output
            //   for directory nodes: 
            s_intersect_count_directory[PE_id], // per page pair
            s_result_pair_directory[PE_id],
            //   for leaf nodes: 
            s_intersect_count_leaf[PE_id], // per page pair
            s_result_pair_leaf[PE_id],
            s_finish_join_PE_out[PE_id]
        );


        // layer cache burst
        burst_buffer(
            // input
            s_result_pair_directory[PE_id], 
            s_finish_join_PE_out[PE_id],

            // output
            s_result_pair_directory_burst_length[PE_id], 
			s_result_pair_directory_burst_contain_last[PE_id],
            s_result_pair_directory_burst[PE_id], 
            s_finish_layer_cache_burst_out[PE_id]
        );

        // result burst
        burst_buffer(
            // input
            s_result_pair_leaf[PE_id], 
            s_finish_layer_cache_burst_out[PE_id], 

            // output
            s_result_pair_leaf_burst_length[PE_id], 
			s_result_pair_leaf_burst_contain_last[PE_id],
            s_result_pair_leaf_burst[PE_id], 
            s_finish_write_burst_out[PE_id]
        );

    }
   
    aggregate_finish_signals<N_JOIN_PE>(
        s_finish_write_burst_out,
        s_finish_join_PE_out_aggregated); 

    layer_cache_memory_controller(
        // input
        //   argument
        max_level_A,
        max_level_B,
        root_id_A,
        root_id_B,
        //   memory 
        layer_cache,
        //   from join PE
        s_intersect_count_directory, 
        s_result_pair_directory_burst_length, 
		s_result_pair_directory_burst_contain_last,
        s_result_pair_directory_burst, 
        //   from scheduler
        axis_layer_cache_read_info, 
        axis_layer_cache_write_addr, 
        axis_num_layer_pairs,
        s_finish_join_PE_out_aggregated,
        // output
        //   to scheduler
        axis_page_pair_scheduler,      // for read request, return pair
        axis_intersect_count_directory_scheduler, // for write request, return count
        s_finish_layer_cache_out
    );

    write_results(
        // input
        //   from join PE
        s_intersect_count_leaf, // per page pair
        s_result_pair_leaf_burst_length, 
		s_result_pair_leaf_burst_contain_last,
        s_result_pair_leaf_burst, 
        //   from the scheduler
        axis_PE_task_count, 
        s_finish_layer_cache_out,  // final end signal 
        // out
        //    out format: the first number writes total intersection count, 
        //                while the rest are intersect ID pairs
        out_intersect
    );
}

}
