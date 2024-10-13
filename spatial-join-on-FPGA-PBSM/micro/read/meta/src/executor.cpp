#include "types.hpp"
#include "PEs.hpp"

extern "C" {

void executor(
        // input

        // A
        const ap_uint<512>* dataset_A,
        const ap_uint<512>* meta_A,
        int num_pages_A,

        // B
        const ap_uint<512>* dataset_B,
        const ap_uint<512>* meta_B,
        int num_pages_B,

        int num_pages_meta,

        // output: count of total intersections, followed by intersecting ID pairs
        ap_uint<64>* out_intersect
) {

#pragma HLS INTERFACE m_axi port=dataset_A offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=meta_A offset=slave bundle=gmem1

#pragma HLS INTERFACE m_axi port=dataset_B offset=slave bundle=gmem2
#pragma HLS INTERFACE m_axi port=meta_B offset=slave bundle=gmem3

#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem4

#pragma HLS dataflow

    // meta

    hls::stream<ap_uint<512>> meta_stream_raw_A;
#pragma HLS stream variable=meta_stream_raw_A depth=512

    hls::stream<ap_uint<512>> meta_stream_raw_B;
#pragma HLS stream variable=meta_stream_raw_B depth=512

    // data

    hls::stream<ap_uint<512>> data_stream_raw_A;
#pragma HLS stream variable=data_stream_raw_A depth=512

    hls::stream<ap_uint<512>> data_stream_raw_B;
#pragma HLS stream variable=data_stream_raw_B depth=512

    // done

    hls::stream<bool> done_stream[4];
#pragma HLS stream variable=done_stream depth=64

    read_data(meta_A, num_pages_meta, meta_stream_raw_A);
    read_data(meta_B, num_pages_meta, meta_stream_raw_B);
    read_data(dataset_A, num_pages_A, data_stream_raw_A);
    read_data(dataset_B, num_pages_B, data_stream_raw_B);

    consume_data(meta_stream_raw_A, num_pages_meta, done_stream[0]);
    consume_data(meta_stream_raw_B, num_pages_meta, done_stream[1]);
    consume_data(data_stream_raw_A, num_pages_A, done_stream[2]);
    consume_data(data_stream_raw_B, num_pages_B, done_stream[3]);

    write_done(
            done_stream,
            out_intersect
            );
    }
}