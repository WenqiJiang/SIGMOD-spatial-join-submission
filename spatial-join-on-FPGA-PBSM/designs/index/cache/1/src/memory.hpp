#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"
#include <hls_stream.h>
#include "hls_burst_maxi.h"

#define CACHE_SIZE 16

struct cache_entry_t {
    int node_offset;
    ap_uint<512> node_data[MAX_NODE_PAGE_NUM];
};

void index_node_read_unit(
        hls::burst_maxi <ap_uint<512>>& tree_data,
        int node_pages,

        hls::stream<int> (&request_stream)[N_JOIN_UNITS],
        hls::stream<ap_uint<512>> (&response_stream)[N_JOIN_UNITS],

        hls::stream<bool> (&done_signal)[N_JOIN_UNITS]
) {
//#pragma HLS INLINE off

    cache_entry_t cache[CACHE_SIZE];
    int access_order[CACHE_SIZE];

    bool PE_finished[N_JOIN_UNITS] = {false};
    int finished_PEs = 0;

    for (int i = 0; i < CACHE_SIZE; i++) {
#pragma HLS UNROLL

        cache[i].node_offset = -1;
        access_order[i] = i;
    }

    while (finished_PEs < N_JOIN_UNITS) {
        for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
            if (!PE_finished[PE_id]) {
                if (!request_stream[PE_id].empty()) {
                    int node_offset = request_stream[PE_id].read();

                    // Stage 2: Cache lookup
                    bool hit = false;
                    int cache_index = -1;

                    for (int j = 0; j < CACHE_SIZE; j++) {
#pragma HLS UNROLL
                        if (cache[j].node_offset == node_offset) {
                            cache_index = j;
                            hit = true;
                            break;
                        }
                    }

                    // Stage 3: Memory Fetch and Cache Update (only if miss)
                    if (!hit) {
                        // LRU Replacement Policy: Select the least recently used entry
                        cache_index = access_order[0];

                        // Fetch the node from memory
                        tree_data.read_request(node_offset, node_pages);
                        for (int k = 0; k < node_pages; k++) {
#pragma HLS PIPELINE II=1
                            auto data = tree_data.read();
                            cache[cache_index].node_data[k] = data;
                            response_stream[PE_id].write(data);
                        }

                        cache[cache_index].node_offset = node_offset;
                    } else {
                        // Stage 4: Send response directly from cache (on hit)
                        for (int k = 0; k < node_pages; k++) {
#pragma HLS PIPELINE II=1
                            response_stream[PE_id].write(cache[cache_index].node_data[k]);
                        }
                    }

                    // Stage 5: Update LRU access order
                    for (int j = 0; j < CACHE_SIZE - 1; j++) {
#pragma HLS UNROLL
                        if (access_order[j] == cache_index) {
                            for (int l = j; l < CACHE_SIZE - 1; l++) {
                                access_order[l] = access_order[l + 1];
                            }
                            break;
                        }
                    }

                    access_order[CACHE_SIZE - 1] = cache_index;

                } else if (!done_signal[PE_id].empty()) {
                    done_signal[PE_id].read();
                    PE_finished[PE_id] = true;
                    finished_PEs++;
                }
            }
        }
    }
}
