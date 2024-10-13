#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"

void read_unit(
        const ap_uint<512>* in_dataset,
        int num_pages,

        hls::stream<ap_uint<512>> (&out_stream)
) {
    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read a burst and write it into stream
        ap_uint<512> obj_group = in_dataset[i];

        out_stream.write(obj_group);
    }
}

void parse_meta_unit(
        // inputs
        hls::stream<ap_uint<512>>& in_metadata,

        int num_meta,
        int num_pages,

        // outputs
        hls::stream<partition_meta_t>& out_stream
) {
    // count written metadata entries
    int meta_written = 0;

    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read page
        ap_uint<512> page = in_metadata.read();

        // count metadata in current page
        int meta_in_current_page = MAX_META_PER_PAGE;

        if (i == num_pages - 1) {
            // last burst -> might be fewer metadata entries
            meta_in_current_page = num_meta - meta_written;
        }

        // read all metadata from the burst (max 5 in each burst = 60B), BUT do not read non-existent metadata
        for (int j = 0; j < meta_in_current_page; j++) {
            ap_uint<32> count_ap_uint_32 = page.range(j * META_SIZE_BITS + 32 * 1 - 1, j * META_SIZE_BITS + 32 * 0);
            ap_uint<32> x_ap_uint_32 = page.range(j * META_SIZE_BITS + 32 * 2 - 1, j * META_SIZE_BITS + 32 * 1);
            ap_uint<32> y_ap_uint_32 = page.range(j * META_SIZE_BITS + 32 * 3 - 1, j * META_SIZE_BITS + 32 * 2);

            partition_meta_t meta;
            meta.count = count_ap_uint_32.to_int();
            meta.x = *reinterpret_cast<float*>(&x_ap_uint_32);
            meta.y = *reinterpret_cast<float*>(&y_ap_uint_32);

            // write to output stream
            out_stream.write(meta);

            meta_written++;
        }
    }

    printf("PARSER parsed %d pages and %d metadata entries \n", num_pages, meta_written);
}

void schedule_unit(
        int num_partitions,

        hls::stream<partition_meta_t>& meta_stream_A,
        hls::stream<partition_meta_t>& meta_stream_B,

        hls::stream<ap_uint<512>>& data_stream_raw_A,
        hls::stream<ap_uint<512>>& data_stream_raw_B,

        hls::stream<ap_uint<512>> (&scheduled_data_stream_raw_A)[N_JOIN_UNITS],
        hls::stream<ap_uint<512>> (&scheduled_data_stream_raw_B)[N_JOIN_UNITS],

        hls::stream<partition_meta_t> (&scheduled_meta_stream_A)[N_JOIN_UNITS],
        hls::stream<partition_meta_t> (&scheduled_meta_stream_B)[N_JOIN_UNITS],

        hls::stream<partition_meta_t> (&scheduled_meta_parse_stream_A)[N_JOIN_UNITS],
        hls::stream<partition_meta_t> (&scheduled_meta_parse_stream_B)[N_JOIN_UNITS],

        hls::stream<bool> (&new_partition_signal)[N_JOIN_UNITS],

        hls::stream<bool> (&FIN_schedule_stream_A)[N_JOIN_UNITS],
        hls::stream<bool> (&FIN_schedule_stream_B)[N_JOIN_UNITS],

        hls::stream<bool> (&FIN_schedule_stream_join)[N_JOIN_UNITS]
) {
    // init PE work tracker
    int PE_ready[N_JOIN_UNITS];
    for (int i = 0; i < N_JOIN_UNITS; i++) {
        PE_ready[i] = 1;
    }

    for (int i = 0; i < num_partitions; i++) {
        partition_meta_t partition_meta_A = meta_stream_A.read();
        partition_meta_t partition_meta_B = meta_stream_B.read();

        int num_pages_A = (partition_meta_A.count + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
        int num_pages_B = (partition_meta_B.count + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;

        bool assigned = false;
        while (!assigned) {
            for (int current_PE = 0; current_PE < N_JOIN_UNITS; current_PE++) {
#pragma HLS PIPELINE II=1

                // check if PE ready update was received
                if (PE_ready[current_PE] == 0 && !new_partition_signal[current_PE].empty() && new_partition_signal[current_PE].read()) {
                    PE_ready[current_PE] = 1;
                }

                if (PE_ready[current_PE]) {
                    // assign partition to this PE
                    PE_ready[current_PE] = 0;

                    printf("Partition to: %d \n", current_PE);

                    scheduled_meta_stream_A[current_PE].write(partition_meta_A);
                    scheduled_meta_stream_B[current_PE].write(partition_meta_B);

                    scheduled_meta_parse_stream_A[current_PE].write(partition_meta_A);
                    scheduled_meta_parse_stream_B[current_PE].write(partition_meta_B);

                    int max_num_pages_count = std::max(num_pages_A, num_pages_B);
                    for (int j = 0; j < max_num_pages_count; j++) {
#pragma HLS pipeline II=1

                        if (num_pages_A > j) {
                            auto data_page_A = data_stream_raw_A.read();
                            scheduled_data_stream_raw_A[current_PE].write(data_page_A);
                        }

                        if (num_pages_B > j) {
                            auto data_page_B = data_stream_raw_B.read();
                            scheduled_data_stream_raw_B[current_PE].write(data_page_B);
                        }
                    }

                    printf("Partition sent!\n");

                    assigned = true;
                    break;
                }
            }
        }
    }

    // if PE_ready[current_PE] is 0, the signal is on the way
    for (int current_PE = 0; current_PE < N_JOIN_UNITS; current_PE++) {
        if (!PE_ready[current_PE]) {
            // wait for the signal
            while (new_partition_signal[current_PE].empty()) {}

            // consume the signal
            new_partition_signal[current_PE].read();
        }
    }

    printf("!Done with scheduling!\n");

    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS pipeline II=1
        FIN_schedule_stream_A[PE_id].write(true);
        FIN_schedule_stream_B[PE_id].write(true);
        FIN_schedule_stream_join[PE_id].write(true);
    }
}

void parse_data_unit(
        // inputs
        hls::stream<ap_uint<512>>& in_dataset,
        hls::stream<partition_meta_t>& meta_stream,
        hls::stream<bool>& FIN_schedule_stream,

        // outputs
        hls::stream<obj_t>& out_stream
) {
    // count written objects
    int objs_written = 0;
    int pages_parsed = 0;

    while (true) {
#pragma HLS pipeline II=1
        if (!meta_stream.empty()) {
            partition_meta_t meta = meta_stream.read();
            int num_objects = meta.count;
            int num_pages = (meta.count + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
            int objs_written_in_partition = 0;

            // read pages
            for (int i = 0; i < num_pages; i++) {
                ap_uint<512> page = in_dataset.read();
                pages_parsed++;

                // count objects in current page
                int objs_in_current_page = MAX_OBJS_PER_PAGE;
                if (i == num_pages - 1) {
                    // last burst -> might be fewer objects
                    objs_in_current_page = num_objects - objs_written_in_partition;
                }

                // read all objects from the burst (max 3 in each burst = 60B), BUT do not read non-existent objects
                for (int j = 0; j < objs_in_current_page; j++) {
                    ap_uint<32> id_A_ap_uint_32 = page.range(j * OBJ_SIZE_BITS + 32 * 1 - 1, j * OBJ_SIZE_BITS + 32 * 0);
                    ap_uint<32> low0_A_ap_uint_32 = page.range(j * OBJ_SIZE_BITS + 32 * 2 - 1, j * OBJ_SIZE_BITS + 32 * 1);
                    ap_uint<32> high0_A_ap_uint_32 = page.range(j * OBJ_SIZE_BITS + 32 * 3 - 1, j * OBJ_SIZE_BITS + 32 * 2);
                    ap_uint<32> low1_A_ap_uint_32 = page.range(j * OBJ_SIZE_BITS + 32 * 4 - 1, j * OBJ_SIZE_BITS + 32 * 3);
                    ap_uint<32> high1_A_ap_uint_32 = page.range(j * OBJ_SIZE_BITS + 32 * 5 - 1, j * OBJ_SIZE_BITS + 32 * 4);

                    obj_t obj_A;
                    obj_A.id = id_A_ap_uint_32.to_int();
                    obj_A.low0 = *reinterpret_cast<float *>(&low0_A_ap_uint_32);
                    obj_A.high0 = *reinterpret_cast<float *>(&high0_A_ap_uint_32);
                    obj_A.low1 = *reinterpret_cast<float *>(&low1_A_ap_uint_32);
                    obj_A.high1 = *reinterpret_cast<float *>(&high1_A_ap_uint_32);

                    // write to output stream
                    out_stream.write(obj_A);

                    objs_written_in_partition++;
                    objs_written++;
                }
            }
        } else if (!FIN_schedule_stream.empty()) {
            FIN_schedule_stream.read();
            break;
        }
    }

    printf("Parser parsed %d pages and %d objects \n", pages_parsed, objs_written);
}


void join_unit(
        // input
        hls::stream<obj_t>& stream_A,
        hls::stream<partition_meta_t>& meta_stream_A,

        hls::stream<obj_t>& stream_B,
        hls::stream<partition_meta_t>& meta_stream_B,

        hls::stream<bool>& FIN_schedule_stream_join,
        hls::stream<bool>& new_partition_signal,

        // output
        hls::stream<result_t>& results,
        hls::stream<bool>& FIN_join
) {
    obj_t cache_B[MAX_OBJS_PER_PARTITION];

    int partitions_joined = 0;

    while (true) {
        if (!meta_stream_A.empty() || !meta_stream_B.empty()) {
            partitions_joined++;

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

                            results.write(result);
                        }
                    }
                }
            }

            new_partition_signal.write(true);

        } else if (!FIN_schedule_stream_join.empty()) {
            printf("Join completed! \n");
            FIN_schedule_stream_join.read();
            break;
        }
    }

    result_t result;
    result.last = true;
    results.write(result);

    // signal that gen is complete
    FIN_join.write(true);
}

template<int count>
void aggregate_signals_unit(
        hls::stream<bool> (&SIG_in)[count],
        hls::stream<bool> &SIG_out
) {
    bool signal = false;

    for (int i = 0; i < count; i++) {
        signal = SIG_in[i].read();
    }

    SIG_out.write(signal);
}
