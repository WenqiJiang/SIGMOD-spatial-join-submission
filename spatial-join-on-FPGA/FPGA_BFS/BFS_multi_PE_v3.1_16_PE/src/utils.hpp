#pragma once

#include "types.hpp"

template<typename T>
inline T block_read(hls::stream<T>& s) {
#pragma HLS inline

    while (s.empty()) {}
    return s.read();
}

// pair_t to ap_uint<64>
ap_uint<64> pack_pair(pair_t input) {
#pragma HLS inline

    ap_uint<64> output_ap_uint_64;
    ap_uint<32> id_A_uint = *((ap_uint<32>*) (&input.id_A));
    ap_uint<32> id_B_uint = *((ap_uint<32>*) (&input.id_B));
    output_ap_uint_64.range(31, 0) = id_A_uint;
    output_ap_uint_64.range(63, 32) = id_B_uint;

    return output_ap_uint_64;
}

// ap_uint<64> to pair_t 
pair_t unpack_pair(ap_uint<64> input) {
#pragma HLS inline

    pair_t output;
    ap_uint<32> id_A_uint = input.range(31, 0);
    ap_uint<32> id_B_uint = input.range(63, 32);
    output.id_A = id_A_uint;
    output.id_B = id_B_uint;

    return output;
}

void pass_termination_signal(
    // from AXIS
    hls::stream<int> &axis_join_finish,
    // to internal PEs
    hls::stream<int> &s_join_finish_replicated) {

    int end = block_read<int>(axis_join_finish);
    s_join_finish_replicated.write(end);
}

void aggregate_join_PE_idle(
    hls::stream<int> (&s_join_PE_idle)[N_JOIN_PE],  // write a signal (1) once a join finishes
    hls::stream<int>& axis_idle_join_PE_ID,
    hls::stream<int> &s_join_finish_in,
    hls::stream<int> &s_join_finish_out
) {
    while (true) {
        for (int PE_id = 0; PE_id < N_JOIN_PE; PE_id++) {
            if (!s_join_PE_idle[PE_id].empty()) {
                int tmp_idle = s_join_PE_idle[PE_id].read();
                axis_idle_join_PE_ID.write(PE_id);
            }
        }
        if (!s_join_finish_in.empty()) {
            int end = s_join_finish_in.read();
            s_join_finish_out.write(end);
            break;
        }
    }
}

template<int n_in>
void aggregate_finish_signals(
    hls::stream<int> (&s_join_finish_in)[n_in],
    hls::stream<int> &s_join_finish_out) {
    // wait for a bunch of join signal to arrive,  
    //  then write a signal indicate all of them are finished
    int finish = 0;
    for (int i = 0; i < n_in; i++) {
        finish = s_join_finish_in[i].read();
    }

    s_join_finish_out.write(finish);
}


template<int n_out>
void broadcast_finish_signals(
    hls::stream<int> &s_join_finish_in,
    hls::stream<int> (&s_join_finish_out)[n_out]) {
    // broadcast a finish signal

    int finish = s_join_finish_in.read();
    for (int i = 0; i < n_out; i++) {
        s_join_finish_out[i].write(finish);
    }
}
