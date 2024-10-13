#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"

void generate_dummy_data(
        hls::stream<obj_t>& stream_A,
        hls::stream<obj_t>& stream_B,
        int num_objects,
        float intersection_percentage
) {
    int num_intersecting_objs = static_cast<int>(num_objects * intersection_percentage);
    int num_non_intersecting_objs = num_objects - num_intersecting_objs;

    for (int i = 0; i < num_objects; i++) {
#pragma HLS pipeline II=1

        obj_t obj_A;

        if (i < num_intersecting_objs) {
            obj_A = {i, 0.0f, 1.0f, 0.0f, 1.0f};
        } else {
            obj_A = {i, 0.0f, 0.5f, 0.0f, 0.5f};
        }

        stream_A.write(obj_A);

        for (int j = 0; j < num_objects; j++) {
            obj_t obj_B;

            if (i < num_intersecting_objs) {
                obj_B = {j, 0.0f, 1.0f, 0.0f, 1.0f};
            } else {
                obj_B = {j, 0.6f, 1.0f, 0.6f, 1.0f};
            }

            stream_B.write(obj_B);
        }
    }
}


// join
void join(
        // A
        int num_objects_A,
        hls::stream<obj_t>& stream_A,

        // B
        int num_objects_B,
        hls::stream<obj_t>& stream_B,

        // output
        hls::stream<result_t>& result_stream,
        hls::stream<bool>& FIN_join_stream
) {

    obj_t obj_A;
    obj_t obj_B;

    for (int i = 0; i < num_objects_A; i++) {
        obj_A = stream_A.read();

        for (int j = 0; j < num_objects_B; j++) {
#pragma HLS pipeline II=1

            obj_B = stream_B.read();

            // check if objects overlap
            bool intersects = (obj_A.low0 <= obj_B.high0 && obj_A.high0 >= obj_B.low0) &&
                              (obj_A.low1 <= obj_B.high1 && obj_A.high1 >= obj_B.low1);

            if (intersects) {
                result_t result;
                result.id_A = obj_A.id;
                result.id_B = obj_B.id;
                result_stream.write(result);
            }
        }
    }

    // send a completion signal
    FIN_join_stream.write(true);
}

void consume_results(
        hls::stream<result_t> &result_stream,
        hls::stream<bool> &FIN_join_stream,
        // output
        ap_uint<64>* out_intersect
) {
    int count = 0;

    while(true) {
        if (!result_stream.empty()) {
            result_t result = result_stream.read();
            count++;
        }
        // check for finish signal
        else if (!FIN_join_stream.empty()) {
            // mark PE as finished
            FIN_join_stream.read();
            break;
        }
    }

    out_intersect[0] = count;
}
