#include "types.hpp"
#include "PEs.hpp"

extern "C" {

void executor(
        int num_partitions,
        int num_objects_per_partition,
        float intersection_percentage,
        ap_uint<64>* out_intersect
) {

#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem4

#pragma HLS dataflow

    hls::stream<obj_t> stream_A;
#pragma HLS stream variable=stream_A depth=512

    hls::stream<obj_t> stream_B;
#pragma HLS stream variable=stream_B depth=512

    hls::stream<partition_meta_t> meta_stream_A;
#pragma HLS stream variable=meta_stream_A depth=512

    hls::stream<partition_meta_t> meta_stream_B;
#pragma HLS stream variable=meta_stream_B depth=512

    hls::stream<bool> FIN_schedule_stream;
#pragma HLS stream variable=FIN_schedule_stream depth=64

    // output stream

    hls::stream<bool> FIN_join_stream;
#pragma HLS stream variable=FIN_join_stream depth=64

    hls::stream<result_t> result_stream;
#pragma HLS stream variable=result_stream depth=512

    generate_dummy_data(
            stream_A,
            meta_stream_A,
            stream_B,
            meta_stream_B,
            FIN_schedule_stream,
            num_partitions,
            num_objects_per_partition,
            intersection_percentage
            );

    join(
            // input
            stream_A,
            meta_stream_A,

            stream_B,
            meta_stream_B,

            // control
            FIN_schedule_stream,

            // output
            result_stream,
            FIN_join_stream
    );

    consume_results(
            // input
            result_stream,
            FIN_join_stream,
            out_intersect
    );
}
}