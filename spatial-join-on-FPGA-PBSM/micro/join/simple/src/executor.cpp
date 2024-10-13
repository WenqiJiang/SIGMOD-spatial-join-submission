#include "types.hpp"
#include "PEs.hpp"

extern "C" {

void executor(
        int num_objects,
        float intersection_percentage,
        ap_uint<64> *out_intersect
) {

#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem4

#pragma HLS dataflow

    hls::stream <obj_t> stream_A;
#pragma HLS stream variable=stream_A depth=512

    hls::stream <obj_t> stream_B;
#pragma HLS stream variable=stream_B depth=512

    // output stream

    hls::stream<bool> FIN_join_stream;
#pragma HLS stream variable=FIN_join_stream depth=64

    hls::stream <result_t> result_stream;
#pragma HLS stream variable=result_stream depth=512

    generate_dummy_data(
            stream_A,
            stream_B,
            num_objects,
            intersection_percentage
    );

    join(
            // input
            num_objects,
            stream_A,

            num_objects,
            stream_B,

            // output
            result_stream,
            FIN_join_stream
    );

    // consume results
    consume_results(
            result_stream,
            FIN_join_stream,
            out_intersect
    );
}
}