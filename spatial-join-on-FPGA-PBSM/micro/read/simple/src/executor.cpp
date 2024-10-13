#include "types.hpp"
#include "PEs.hpp"

extern "C" {

void executor(
        // input

        // A
        const ap_uint<512>* dataset,
        int num_pages,

        // output: count of total intersections, followed by intersecting ID pairs
        ap_uint<64>* out_intersect
) {

#pragma HLS INTERFACE m_axi port=dataset offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem4

#pragma HLS dataflow

    // data

    hls::stream<ap_uint<512>> data_stream_raw;
#pragma HLS stream variable=data_stream_raw depth=512

    // done

    hls::stream<bool> done_stream;
#pragma HLS stream variable=done_stream depth=2

    read_data(dataset, num_pages, data_stream_raw);

    consume_data(data_stream_raw, num_pages, done_stream);

    write_done(
            done_stream,
            out_intersect
    );
}
}