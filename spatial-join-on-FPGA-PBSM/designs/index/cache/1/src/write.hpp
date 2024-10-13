#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"
#include "hls_burst_maxi.h"

void collect_unit(
        // input streams from a join unit
        hls::stream<result_t>& results,
        hls::stream<bool>& FIN,

        // output streams to the writer
        hls::stream<result_t>& writer_burst,
        hls::stream<int>& writer_burst_count,
        hls::stream<bool>& FIN_out
) {
    while (true) {
        if (!results.empty()) {
            int burst_length = MAX_BURST_LENGTH;

            // collect data from the result stream
            for (int i = 0; i < MAX_BURST_LENGTH; i++) {
#pragma HLS pipeline II=1

                result_t result = results.read();

                // if 'last' signal is received, terminate the burst early
                if (result.last) {
                    burst_length = i; // last does not count
                    break;
                } else {
                    // only write non-last content
                    writer_burst.write(result);
                }
            }

            // send the number of elements in the burst
            writer_burst_count.write(burst_length);
        }
        else if (!FIN.empty()) {
            int finished = FIN.read();
            FIN_out.write(finished);
            break;
        }
    }
}

// result_t to ap_uint<64>
ap_uint<64> pack_pair(result_t result) {
#pragma HLS inline

    ap_uint<64> output_ap_uint_64;
    ap_uint<32> id_A_uint = *((ap_uint<32>*) (&result.id_A));
    ap_uint<32> id_B_uint = *((ap_uint<32>*) (&result.id_B));
    output_ap_uint_64.range(31, 0) = id_A_uint;
    output_ap_uint_64.range(63, 32) = id_B_uint;

    return output_ap_uint_64;
}

void write_unit(
        hls::stream<result_t> (&bursts_buffer_stream)[N_JOIN_UNITS],
        hls::stream<int> (&bursts_length_stream)[N_JOIN_UNITS],
        hls::stream<bool> (&FIN),
        hls::burst_maxi<ap_uint<64>> out_intersect
) {
    ap_uint<64> count = 1;

    int PE_cache[MAX_BURSTS_PER_WRITE_REQUEST];
    int burst_length_cache[MAX_BURSTS_PER_WRITE_REQUEST];

    while (true) {
        bool any_data_written = false;
        int bursts_count = 0;
        int request_length = 0;

        for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
            // compute all bursts' lengths to send in a request
            while (!bursts_length_stream[PE_id].empty() && bursts_count < MAX_BURSTS_PER_WRITE_REQUEST) {
                // read from the burst
                any_data_written = true;

                int burst_length = bursts_length_stream[PE_id].read();

                if (burst_length > 0) {
                    PE_cache[bursts_count] = PE_id;
                    burst_length_cache[bursts_count] = burst_length;
                    request_length += burst_length;
                    bursts_count++;
                }
            }
        }

        // now the length for the request is know, so send data
        if (request_length > 0) {
            out_intersect.write_request(count, request_length);

            for (int i = 0; i < bursts_count; i++) {
                int burst_length = burst_length_cache[i];
                int PE_id = PE_cache[i];

                for (int i = 0; i < burst_length; i++) {
#pragma HLS pipeline II=1

                    result_t result = bursts_buffer_stream[PE_id].read();
                    ap_uint<64> packed_result = pack_pair(result);
                    out_intersect.write(packed_result);
                }
            }

            count += request_length;
            out_intersect.write_response();
        }

        // data was sent

        // check if done
        if (!any_data_written && !FIN.empty()) {

            bool has_content_recheck = false;

            // double check
            for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
                if (!bursts_buffer_stream[PE_id].empty() || !bursts_length_stream[PE_id].empty()) {
                    has_content_recheck = true;
                    // Wenqi: no break here, all the empty signals will be and in a single cycle,
                    //   and the break will break the outer while loop instead of this for loop
                    //   (HLS behavior problem)
                    ////// break;
                }
            }

            if (!has_content_recheck) {
                FIN.read();
                break;
            }
        }
    }

    // total number of results
    out_intersect.write_request(0, 1);
    out_intersect.write(count - 1);
    out_intersect.write_response();
}
