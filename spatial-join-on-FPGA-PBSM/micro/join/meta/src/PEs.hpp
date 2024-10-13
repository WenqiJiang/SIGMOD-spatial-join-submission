#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"

void generate_dummy_data(
        hls::stream<obj_t>& stream_A,
        hls::stream<partition_meta_t>& meta_stream_A,
        hls::stream<obj_t>& stream_B,
        hls::stream<partition_meta_t>& meta_stream_B,
        hls::stream<bool>& FIN_schedule_stream,
        int num_partitions,
        int num_objs_per_partition,
        float intersection_percentage
) {
    int num_intersecting_objs = static_cast<int>(num_objs_per_partition * intersection_percentage);
    int num_non_intersecting_objs = num_objs_per_partition - num_intersecting_objs;

    // generate partition metadata
    for (int i = 0; i < num_partitions; i++) {
#pragma HLS pipeline II=1

        partition_meta_t meta_A = {num_objs_per_partition, 0.0f, 0.0f};
        partition_meta_t meta_B = {num_objs_per_partition, 0.0f, 0.0f};
        meta_stream_A.write(meta_A);
        meta_stream_B.write(meta_B);

        // generate intersecting objs for A and B
        for (int j = 0; j < num_intersecting_objs; j++) {
            obj_t obj_A = {j, 0.0f, 1.0f, 0.0f, 1.0f};
            obj_t obj_B = {j, 0.0f, 1.0f, 0.0f, 1.0f};
            stream_A.write(obj_A);
            stream_B.write(obj_B);
        }

        // non-intersecting
        for (int j = num_intersecting_objs; j < num_objs_per_partition; j++) {
            obj_t obj_A = {j, 0.0f, 0.5f, 0.0f, 0.5f};
            obj_t obj_B = {j, 0.6f, 1.0f, 0.6f, 1.0f};
            stream_A.write(obj_A);
            stream_B.write(obj_B);
        }
    }

    // FIN signal
    FIN_schedule_stream.write(true);
}

void join(
        // input
        hls::stream<obj_t>& stream_A,
        hls::stream<partition_meta_t>& meta_stream_A,

        hls::stream<obj_t>& stream_B,
        hls::stream<partition_meta_t>& meta_stream_B,

        hls::stream<bool>& FIN_schedule_stream,

        // output
        hls::stream<result_t>& result_stream,
        hls::stream<bool>& FIN_join_stream
) {
    obj_t cache_B[MAX_OBJS_PER_PARTITION];

    while (true) {
        if (!meta_stream_A.empty() || !meta_stream_B.empty()) {

            partition_meta_t meta_A = meta_stream_A.read();
            partition_meta_t meta_B = meta_stream_B.read();

            // stream B -> cache
            for (int i = 0; i < meta_B.count; i++) {
#pragma HLS pipeline II=1
                cache_B[i] = stream_B.read();
            }

            // join
            for (int i = 0; i <  meta_A.count; i++) {
                obj_t obj_A = stream_A.read();

                for (int j = 0; j <  meta_B.count; j++) {
#pragma HLS pipeline II=1

                    obj_t obj_B = cache_B[j];

                    bool intersects = (obj_A.low0 <= obj_B.high0 && obj_A.high0 >= obj_B.low0) &&
                                      (obj_A.low1 <= obj_B.high1 && obj_A.high1 >= obj_B.low1);

                    if (intersects) {
                        float bottom_left_intersection_x = std::max(obj_A.low0, obj_B.low0);
                        float bottom_left_intersection_y = std::max(obj_A.low1, obj_B.low1);

                        bool within_cell = (bottom_left_intersection_x >= meta_A.x) &&
                                           (bottom_left_intersection_y >= meta_A.y) &&
                                           (bottom_left_intersection_x <= meta_B.x) &&
                                           (bottom_left_intersection_y <= meta_B.y);

                        if (within_cell) {
                            result_t result;
                            result.id_A = obj_A.id;
                            result.id_B = obj_B.id;
                            result_stream.write(result);
                        }
                    }
                }
            }
        } else if (!FIN_schedule_stream.empty()) {
            FIN_schedule_stream.read();
            FIN_join_stream.write(true);
            break;
        }
    }
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
