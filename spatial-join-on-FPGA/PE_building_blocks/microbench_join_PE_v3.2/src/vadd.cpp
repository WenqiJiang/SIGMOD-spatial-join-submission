#include "constants.hpp"
#include "DRAM_utils.hpp"
#include "join_PE.hpp"
#include "types.hpp"

extern "C" {

void vadd(  
    int pair_num, // number of page pairs to join
    int	page_bytes, // typically fixed as 4096
	int max_entry_num, // set during indexing

    // in runtime (should from DRAM)
    const ap_uint<512>* in_pages_A,
    const ap_uint<512>* in_pages_B,
    // out format: the first number writes total intersection count, 
    //   while the rest are intersect ID pairs
    ap_uint<64>* out_intersect 
    )
{
// Share the same AXI interface with several control signals (but they are not allowed in same dataflow)
//    https://docs.xilinx.com/r/en-US/ug1399-vitis-hls/Controlling-AXI4-Burst-Behavior

// in runtime (should from DRAM)
#pragma HLS INTERFACE m_axi port=in_pages_A offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=in_pages_B offset=slave bundle=gmem1

// out
#pragma HLS INTERFACE m_axi port=out_intersect  offset=slave bundle=gmem2

#pragma HLS dataflow

////////////////////     First Half: ADC     ////////////////////


    hls::stream<ap_uint<512> > s_page_A_raw;
#pragma HLS stream variable=s_page_A_raw depth=512
    hls::stream<ap_uint<512> > s_page_B_raw;
#pragma HLS stream variable=s_page_B_raw depth=512

    hls::stream<node_meta_t> s_meta_A;
#pragma HLS stream variable=s_meta_A depth=32
    hls::stream<node_meta_t> s_meta_B;
#pragma HLS stream variable=s_meta_B depth=32

    hls::stream<obj_t> s_page_A;
#pragma HLS stream variable=s_page_A depth=512
    hls::stream<obj_t> s_page_B;
#pragma HLS stream variable=s_page_B depth=512


    hls::stream<int> s_intersect_count_directory;
#pragma HLS stream variable=s_intersect_count_directory depth=32

    hls::stream<result_t> s_result_pair_directory;
#pragma HLS stream variable=s_result_pair_directory depth=512

    hls::stream<int> s_intersect_count_leaf; 
#pragma HLS stream variable=s_intersect_count_leaf depth=32

    hls::stream<result_t> s_result_pair_leaf;
#pragma HLS stream variable=s_result_pair_leaf depth=512

    hls::stream<int> s_finish_read_out; 
#pragma HLS stream variable=s_finish_read_out depth=2

    hls::stream<int> s_finish_parse_page_out; 
#pragma HLS stream variable=s_finish_parse_page_out depth=2

    hls::stream<int> s_finish_join_PE_out; 
#pragma HLS stream variable=s_finish_join_PE_out depth=2

	// load the inputs
	read_nodes(
    	// input
    	pair_num,
    	page_bytes, // typically fixed as 4096
		max_entry_num, // set during indexing
    	in_pages_A,
    	in_pages_B,
		// output
		s_page_A_raw,
		s_page_B_raw,
		s_finish_read_out
		);

	parse_page(
		// input
		max_entry_num, // set during indexing
		s_page_A_raw,
		s_page_B_raw,
		s_finish_read_out,
		// output
		s_meta_A,
		s_meta_B,
		s_page_A,
		s_page_B,
		s_finish_parse_page_out);

	join_page(
		// input
		s_meta_A,
		s_meta_B,
		s_page_A,
		s_page_B,
		s_finish_parse_page_out,
		// output
		//   for directory nodes: 
		s_intersect_count_directory, // per page pair
		s_result_pair_directory,
		//   for leaf nodes: 
		s_intersect_count_leaf, // per page pair
		s_result_pair_leaf,
		s_finish_join_PE_out);
		  
	write_results(
		s_intersect_count_directory, // per page pair
		s_result_pair_directory,
		//   for leaf nodes: 
		s_intersect_count_leaf, // per page pair
		s_result_pair_leaf,
		
		s_finish_join_PE_out,
		// output
		out_intersect);

}

}
