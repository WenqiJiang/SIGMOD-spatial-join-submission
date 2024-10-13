#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "hls_burst_maxi.h"
#include "constants.hpp"

void read_data(
        const ap_uint<512>* in_dataset,
        int num_pages,

        hls::stream<ap_uint<512>> (&out_stream)
) {
    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read a page and write it into stream
        ap_uint<512> obj_group = in_dataset[i];

        out_stream.write(obj_group);
    }
}

void consume_data(
        hls::stream<ap_uint<512>>& in_stream,
        int num_pages,
        hls::stream<bool>& done_stream
) {
    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read and do nothing
        ap_uint<512> data = in_stream.read();
    }

    done_stream.write(true);
}

void write_done(
        hls::stream<bool>& done_stream,
        ap_uint<64>* out_intersect
        ) {

    done_stream.read();
    out_intersect[0] = 1;
}