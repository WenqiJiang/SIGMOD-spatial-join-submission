#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"
#include <hls_stream.h>
#include "hls_burst_maxi.h"

void index_node_read_unit(
        hls::burst_maxi <ap_uint<512>>& tree_data,
        int node_pages,

        hls::stream<int> (&request_stream)[N_JOIN_UNITS],
        hls::stream<ap_uint<512>> (&response_stream)[N_JOIN_UNITS],

        hls::stream<bool> (&done_signal)[N_JOIN_UNITS]
) {
    bool PE_finished[N_JOIN_UNITS] = {false};
    int finished_PEs = 0;

    while (finished_PEs < N_JOIN_UNITS) {
        for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
            if (!PE_finished[PE_id]) {
                if (!request_stream[PE_id].empty()) {
                    int node_offset = request_stream[PE_id].read();

                    tree_data.read_request(node_offset, node_pages);

                    for (int i = 0; i < node_pages; i++) {
#pragma HLS pipeline II=1

                        auto data = tree_data.read();
                        response_stream[PE_id].write(data);
                    }

                } else if (!done_signal[PE_id].empty()) {
                    done_signal[PE_id].read();
                    PE_finished[PE_id] = true;

                    finished_PEs++;
                }
            }
        }
    }
}
