#pragma once

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "types.hpp"
#include <hls_stream.h>
#include "hls_burst_maxi.h"

void read_unit(
        // inputs
        const ap_uint<512>* in_dataset,
        int num_pages,
        // output
        hls::stream<ap_uint<512>> (&out_stream)
) {
    for (int i = 0; i < num_pages; i++) {
#pragma HLS pipeline II=1

        ap_uint<512> obj_group = in_dataset[i];
        out_stream.write(obj_group);
    }
}

void schedule_unit(
        int num_pages,
        int num_objects,
        hls::stream<ap_uint<512>>& data_stream_raw,
        hls::stream<bool> (&done_signal)[N_JOIN_UNITS],

        hls::stream<task_t> (&task_stream_parse)[N_JOIN_UNITS],
        hls::stream<int> (&task_stream_join)[N_JOIN_UNITS],

        hls::stream<bool> (&FIN_schedule_signal_parse)[N_JOIN_UNITS],
        hls::stream<bool> (&FIN_schedule_signal_join)[N_JOIN_UNITS]
) {
    int objects_in_last_page = num_objects % MAX_OBJS_PER_PAGE;

    int PE_ready[N_JOIN_UNITS];
    for (int i = 0; i < N_JOIN_UNITS; i++) {
        PE_ready[i] = 1;
    }

    for (int i = 0; i < num_pages; i++) {
        bool assigned = false;

        while (!assigned) {
            for (int current_PE = 0; current_PE < N_JOIN_UNITS; current_PE++) {
#pragma HLS PIPELINE II=1

                // check if PE ready update was received
                if (PE_ready[current_PE] == 0 && !done_signal[current_PE].empty() && done_signal[current_PE].read()) {
                    PE_ready[current_PE] = 1;
                }

                if (PE_ready[current_PE]) {
                    PE_ready[current_PE] = 0;

                    auto page = data_stream_raw.read();
                    int objects = i == num_pages - 1 ? objects_in_last_page : MAX_OBJS_PER_PAGE;
                    task_t new_task = { page, objects };
                    task_stream_parse[current_PE].write(new_task);
                    task_stream_join[current_PE].write(objects);

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
            while (done_signal[current_PE].empty()) {}

            // consume the signal
            done_signal[current_PE].read();
        }
    }

    printf("!Done with scheduling!\n");

    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS pipeline II=1
        FIN_schedule_signal_parse[PE_id].write(true);
        FIN_schedule_signal_join[PE_id].write(true);
    }
}

void parse_data_unit(
        // inputs
        hls::stream<task_t>& task_stream,
        hls::stream<bool> (&FIN_schedule_signal_parse),
        // outputs
        hls::stream<obj_t>& out_stream
) {
    while (true) {
#pragma HLS pipeline II=1
        if (!task_stream.empty()) {
            auto task = task_stream.read();
            auto page = task.page;

            // parse all objects from the page
            for (int j = 0; j < task.num_objects; j++) {
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
            }
        } else if (!FIN_schedule_signal_parse.empty()) {
            FIN_schedule_signal_parse.read();
            break;
        }
    }
}

// parse meta data
node_meta_t parse_node_meta_unit(
        ap_uint<512> data
) {
#pragma HLS inline

    node_meta_t meta_data;

    ap_uint<32> reg_is_leaf_uint32 = data.range(32 * 0 + 31, 32 * 0);
    meta_data.is_leaf = static_cast<int>(reg_is_leaf_uint32.to_uint());

    ap_uint<32> reg_count_uint32 = data.range(32 * 1 + 31, 32 * 1);
    meta_data.count = *((int*) (&reg_count_uint32));

    ap_uint<32> reg_obj_id_uint32 = data.range(32 * 2 + 31, 32 * 2);
    meta_data.obj.id = *((int*) (&reg_obj_id_uint32));

    ap_uint<32> reg_obj_low0_uint32 = data.range(32 * 3 + 31, 32 * 3);
    meta_data.obj.low0 = *((float*) (&reg_obj_low0_uint32));

    ap_uint<32> reg_obj_high0_uint32 = data.range(32 * 4 + 31, 32 * 4);
    meta_data.obj.high0 = *((float*) (&reg_obj_high0_uint32));

    ap_uint<32> reg_obj_low1_uint32 = data.range(32 * 5 + 31, 32 * 5);
    meta_data.obj.low1 = *((float*) (&reg_obj_low1_uint32));

    ap_uint<32> reg_obj_high1_uint32 = data.range(32 * 6 + 31, 32 * 6);
    meta_data.obj.high1 = *((float*) (&reg_obj_high1_uint32));

    return meta_data;
}

obj_t parse_obj_unit(
        const ap_uint<512>& group,
        int j
) {
    obj_t obj;

    ap_uint<32> id_A_ap_uint_32 = group.range(j * OBJ_SIZE_BITS + 32 * 1 - 1, j * OBJ_SIZE_BITS + 32 * 0);
    ap_uint<32> low0_A_ap_uint_32 = group.range(j * OBJ_SIZE_BITS + 32 * 2 - 1, j * OBJ_SIZE_BITS + 32 * 1);
    ap_uint<32> high0_A_ap_uint_32 = group.range(j * OBJ_SIZE_BITS + 32 * 3 - 1, j * OBJ_SIZE_BITS + 32 * 2);
    ap_uint<32> low1_A_ap_uint_32 = group.range(j * OBJ_SIZE_BITS + 32 * 4 - 1, j * OBJ_SIZE_BITS + 32 * 3);
    ap_uint<32> high1_A_ap_uint_32 = group.range(j * OBJ_SIZE_BITS + 32 * 5 - 1, j * OBJ_SIZE_BITS + 32 * 4);

    obj.id = id_A_ap_uint_32.to_int();
    obj.low0 = *reinterpret_cast<float*>(&low0_A_ap_uint_32);
    obj.high0 = *reinterpret_cast<float*>(&high0_A_ap_uint_32);
    obj.low1 = *reinterpret_cast<float*>(&low1_A_ap_uint_32);
    obj.high1 = *reinterpret_cast<float*>(&high1_A_ap_uint_32);

    // write to output stream
    return obj;
}

// check if two objects intersect
bool intersects(const obj_t& a, const obj_t& b) {
#pragma HLS inline
    return (a.low0 <= b.high0 && a.high0 >= b.low0) && (a.low1 <= b.high1 && a.high1 >= b.low1);
}

void join_unit(
        hls::stream<int>& task_stream_join,

        // A
        hls::stream<obj_t>& stream_parsed_A,
        hls::stream<bool> (&FIN_schedule_signal_join),

        // B
        int root_id,
        int node_pages,

        hls::stream<int>& request_stream,
        hls::stream<ap_uint<512>>& response_stream,

        hls::stream<bool>& done_signal,

        // output
        hls::stream<result_t>& results,
        hls::stream<bool>& FIN_join,
        hls::stream<bool>& FIN_join_cache
) {
    int groups = node_pages - 1;

    ap_uint<512> node[MAX_NODE_PAGE_NUM];

    hls::stream<int> node_queue;
#pragma HLS stream variable=node_queue depth=512

    while (true) {
        if (!task_stream_join.empty()) {
            int num_objects = task_stream_join.read();

            for (int t = 0; t < num_objects; t++) {
                obj_t target = stream_parsed_A.read();

                printf("Traverse and look for intersection with: %d\n", target.id);
                printf("MBR: %g  %g  %g  %g\n", target.low0, target.high0, target.low1, target.high1);

                node_queue.write(root_id);

                while (!node_queue.empty()) {
                    int node_id = node_queue.read();
                    int node_offset = node_id * node_pages;

                    request_stream.write(node_offset);

                    // request node data from cache
                    for (int i = 0; i < node_pages; i++) {
    #pragma HLS pipeline II=1

                        node[i] = response_stream.read();
                    }

                    printf("Node: id %d, offset %d\n", node_id, node_offset);

                    // parse node's metadata
                    node_meta_t parsed_meta = parse_node_meta_unit(node[0]);

                    // number of object groups in the node
                    printf("Node metadata: is_leaf %d, count %d, groups %d\n", parsed_meta.is_leaf, parsed_meta.count,
                           groups);

                    // test with node's MBR before testing children
                    if (!intersects(target, parsed_meta.obj)) {
                        continue;
                    }

                    int obj_read = 0;

                    // read each group's entries
                    for (int i = 0; i < groups; i++) {
                        // read group
                        ap_uint<512> group_data = node[i + 1];

                        if (obj_read >= parsed_meta.count) {
                            continue;
                        }

                        // read objects
                        for (int j = 0; j < MAX_OBJS_PER_PAGE && (i * MAX_OBJS_PER_PAGE + j) < parsed_meta.count; j++) {
                            obj_t parsed_obj = parse_obj_unit(group_data, j);

                            if (intersects(target, parsed_obj)) {
                                if (parsed_meta.is_leaf) {
                                    // check for matches
                                    result_t result = {target.id, parsed_obj.id, false };
                                    results.write(result);
                                } else {
                                    // enqueue next tasks
                                    node_queue.write(parsed_obj.id);
                                }
                            }
                        }

                        obj_read++;
                    }
                }
            }

            done_signal.write(true);
        } else if (!FIN_schedule_signal_join.empty()) {
            FIN_schedule_signal_join.read();
            break;
        }
    }

    result_t result;
    result.last = true;
    results.write(result);

    // signal completion
    FIN_join.write(true);
    FIN_join_cache.write(true);
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
