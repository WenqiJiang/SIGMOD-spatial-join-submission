#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include "constants.hpp"


config_t compute_scheduling(
        int num_objects_A,
        int num_pages_A
        ) {
#pragma HLS inline

    config_t config;

    // maximum number dataset B needs to be restreamed
    // e.g. if each PE gets at most 9 objects to work on, then B needs to be restreamed at most 9 times
    config.max_num_restreams = MAX_OBJS_PER_PAGE * std::ceil(static_cast<double>(num_pages_A) / N_JOIN_UNITS);

    int base_num_pages_of_A_per_PE = num_pages_A / N_JOIN_UNITS;
    int extra_pages_of_A = num_pages_A % N_JOIN_UNITS;

    int objects_in_last_page = num_objects_A % MAX_OBJS_PER_PAGE;
    int last_obj_PE = (num_pages_A - 1) % N_JOIN_UNITS;

    // distribution of pages and objects to the parallel PEs
    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
        int num_pages_of_PE = base_num_pages_of_A_per_PE + (PE_id < extra_pages_of_A ? 1 : 0);
        int num_objects_of_PE = num_pages_of_PE * MAX_OBJS_PER_PAGE - (PE_id == last_obj_PE ? MAX_OBJS_PER_PAGE - objects_in_last_page : 0);

        config.num_objects_per_PE[PE_id] = num_objects_of_PE;
        config.num_pages_per_PE[PE_id] = num_pages_of_PE;

        printf("PE #%d: pages - %d, objects - %d \n", PE_id, num_pages_of_PE, num_objects_of_PE);
    }

    return config;
}

void read_unit(
        const ap_uint<512>* in_dataset,
        int num_objects,
        int num_pages,

        // outputs
        hls::stream<ap_uint<512>> (&out_stream)[N_JOIN_UNITS]
) {
    int current_PE = 0;

    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read a page and write it into stream
        ap_uint<512> obj_group = in_dataset[i];

        out_stream[current_PE].write(obj_group);
        current_PE = (current_PE + 1) & (N_JOIN_UNITS - 1);
    }
}

void read_and_restream_unit(
        // inputs
        const ap_uint<512>* in_dataset,
        int num_pages,

        int num_objects_A,
        int num_pages_A,

        // outputs
        hls::stream<ap_uint<512>> (&out_stream)[N_JOIN_UNITS]
) {
    auto config = compute_scheduling(num_objects_A, num_pages_A);

    for (int restream = 0; restream < config.max_num_restreams; restream++) {

        for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

            // read a page and write it into stream
            ap_uint<512> obj_group = in_dataset[i];

            // broadcast page
            for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS unroll

                if (restream >= config.num_objects_per_PE[PE_id]) {
                    continue;
                }

                out_stream[PE_id].write(obj_group);
            }
        }
    }
}

void parse_pages_common(
        hls::stream<ap_uint<512>>& in_dataset,
        int num_objects,
        int num_pages,
        hls::stream<obj_t>& out_stream
) {
#pragma HLS inline

    // count written objects
    int objs_written = 0;

    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        // read page
        ap_uint<512> page = in_dataset.read();

        // count objects in current page
        int objs_in_current_page = MAX_OBJS_PER_PAGE;

        if (i == num_pages - 1) {
            // last burst -> might be fewer objects
            objs_in_current_page = num_objects - objs_written;
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
            obj_A.low0 = *reinterpret_cast<float*>(&low0_A_ap_uint_32);
            obj_A.high0 = *reinterpret_cast<float*>(&high0_A_ap_uint_32);
            obj_A.low1 = *reinterpret_cast<float*>(&low1_A_ap_uint_32);
            obj_A.high1 = *reinterpret_cast<float*>(&high1_A_ap_uint_32);

            // write to output stream
            out_stream.write(obj_A);

            objs_written++;
        }
    }
}

void parse_unit(
        hls::stream<ap_uint<512>>& in_dataset,
        int num_objects,
        int num_pages,
        hls::stream<obj_t>& out_stream
) {
    parse_pages_common(in_dataset, num_objects, num_pages, out_stream);
}

void parse_restreamed_unit(
        hls::stream<ap_uint<512>>& in_dataset,
        int num_objects,
        int num_pages,
        int num_objects_of_PE,
        hls::stream<obj_t>& out_stream
) {
    for (int restream = 0; restream < num_objects_of_PE; restream++) {
        parse_pages_common(in_dataset, num_objects, num_pages, out_stream);
    }
}

void join_unit(
        // input

        // A
        int num_objects_of_PE,
        hls::stream<obj_t>& stream_A,

        // B
        int num_objects_B,
        hls::stream<obj_t>& stream_B,

        // output
        hls::stream<result_t>& results,
        hls::stream<bool>& FIN_join_for_collect,
        hls::stream<bool>& FIN_join_for_write
        ) {

    obj_t obj_A;
    obj_t obj_B;

    for (int i = 0; i < num_objects_of_PE; i++) {
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
                result.last = false;
                results.write(result);
            }
        }
    }

    result_t result;
    result.last = true;
    results.write(result);

    // signal completion
    FIN_join_for_collect.write(true);
    FIN_join_for_write.write(true);
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
