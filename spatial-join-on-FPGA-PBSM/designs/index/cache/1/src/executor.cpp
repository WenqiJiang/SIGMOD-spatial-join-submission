#include "types.hpp"
#include "PEs.hpp"
#include "memory.hpp"
#include "write.hpp"

extern "C" {

void executor(
        // dataset A
        int num_objects_A,
        int num_pages_A,
        const ap_uint<512>* dataset_A,

        // dataset (tree) B
        int tree_max_level_B,
        int root_id_B,
        int node_pages,
        int tree_max_node_entries_count_B,
        hls::burst_maxi <ap_uint<512>> buffer_tree_B,

        hls::burst_maxi<ap_uint<64>> out_intersect
) {
#pragma HLS INTERFACE m_axi port=dataset_A offset=slave bundle=gmem0

#pragma HLS INTERFACE m_axi port=buffer_tree_B offset=slave bundle=gmem1

#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem2

#pragma HLS dataflow

    // outer dataset

    hls::stream<ap_uint<512>> stream_raw_A;
#pragma HLS stream variable=stream_raw_A depth=512

    // scheduler

    hls::stream<task_t> task_stream_parse[N_JOIN_UNITS];
#pragma HLS stream variable=task_stream_parse depth=512

    hls::stream<int> task_stream_join[N_JOIN_UNITS];
#pragma HLS stream variable=task_stream_join depth=512

    hls::stream<bool> FIN_schedule_signal_parse[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_schedule_signal_parse depth=2

    hls::stream<bool> FIN_schedule_signal_join[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_schedule_signal_join depth=2

    hls::stream<bool> done_signal[N_JOIN_UNITS];
#pragma HLS stream variable=done_signal depth=2

    hls::stream<bool> FIN_join_cache[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_join_cache depth=2

    // parsed

    hls::stream <obj_t> stream_parsed_A[N_JOIN_UNITS];
#pragma HLS stream variable=stream_parsed_A depth=512

    // cache
    hls::stream<int> request_stream[N_JOIN_UNITS];
#pragma HLS stream variable=request_stream depth=512

    hls::stream<ap_uint<512>> response_stream[N_JOIN_UNITS];
#pragma HLS stream variable=response_stream depth=512

    // output streams

    hls::stream<bool> FIN_join[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_join depth=64

    hls::stream<bool> FIN_collect[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_collect depth=64

    hls::stream<bool> FIN_agg;
#pragma HLS stream variable=FIN_agg depth=64

    hls::stream<result_t> results[N_JOIN_UNITS];
#pragma HLS stream variable=results depth=512

    hls::stream<result_t> writer_burst[N_JOIN_UNITS];
#pragma HLS stream variable=writer_burst depth=512

    hls::stream<int> writer_burst_count[N_JOIN_UNITS];
#pragma HLS stream variable=writer_burst_count depth=512

    // read A
    read_unit(
        dataset_A,
        num_pages_A,
        stream_raw_A
    );

    schedule_unit(
        num_pages_A,
        num_objects_A,
        stream_raw_A,
        done_signal,
        task_stream_parse,
        task_stream_join,
        FIN_schedule_signal_parse,
        FIN_schedule_signal_join
    );

    index_node_read_unit(
        buffer_tree_B,
        node_pages,
        request_stream,
        response_stream,
        FIN_join_cache
    );

    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS unroll

        // parse PE's A pages into objects
        parse_data_unit(
            task_stream_parse[PE_id],
            FIN_schedule_signal_parse[PE_id],
            stream_parsed_A[PE_id]
        );

        // find matching objects in B (index)
        join_unit(
            task_stream_join[PE_id],
            // A
            stream_parsed_A[PE_id],
            FIN_schedule_signal_join[PE_id],

            // B
            root_id_B,
            node_pages,

            request_stream[PE_id],
            response_stream[PE_id],

            done_signal[PE_id],

            // output
            results[PE_id],
            FIN_join[PE_id],
            FIN_join_cache[PE_id]
        );

        // collect results into a burst buffer (FIFO pipe)
        collect_unit(
                // input
                results[PE_id],
                FIN_join[PE_id],

                // output
                writer_burst[PE_id],
                writer_burst_count[PE_id],
                FIN_collect[PE_id]
        );
    }

    aggregate_signals_unit<N_JOIN_UNITS>(
            FIN_collect,
            FIN_agg
    );

    // write results
    write_unit(
            // input
            writer_burst,
            writer_burst_count,
            FIN_agg,

            // output
            out_intersect
    );
}
}