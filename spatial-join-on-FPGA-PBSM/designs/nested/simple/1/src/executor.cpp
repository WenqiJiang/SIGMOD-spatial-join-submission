#include "types.hpp"
#include "PEs.hpp"
#include "write.hpp"

extern "C" {

void executor(
    // dataset A
    int num_objects_A,
    int num_pages_A,
    const ap_uint<512>* dataset_A,

    // dataset B
    int num_objects_B,
    int num_pages_B,
    const ap_uint<512>* dataset_B,

    // output: count of total intersections, followed by intersecting ID pairs
    hls::burst_maxi<ap_uint<64>> out_intersect
) {
#pragma HLS INTERFACE m_axi port=dataset_A offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=dataset_B offset=slave bundle=gmem1
#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem2

#pragma HLS dataflow

    // outer dataset (stream A)
    hls::stream<ap_uint<512>> stream_raw_A[N_JOIN_UNITS];
#pragma HLS stream variable=stream_raw_A depth=512

    hls::stream <obj_t> stream_parsed_A[N_JOIN_UNITS];
#pragma HLS stream variable=stream_parsed_A depth=512

    // inner dataset (stream B)
    hls::stream<ap_uint<512>> stream_raw_B[N_JOIN_UNITS];
#pragma HLS stream variable=stream_raw_B depth=512

    hls::stream <obj_t> stream_parsed_B[N_JOIN_UNITS];
#pragma HLS stream variable=stream_parsed_B depth=512

    // output streams

    hls::stream<bool> FIN_join_for_collect[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_join_for_collect depth=64

    hls::stream<bool> FIN_join_for_write[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_join_for_write depth=64

    hls::stream<bool> FIN_join_agg;
#pragma HLS stream variable=FIN_join_agg depth=64

    hls::stream<result_t> results[N_JOIN_UNITS];
#pragma HLS stream variable=results depth=512

    hls::stream<result_t> writer_burst[N_JOIN_UNITS];
#pragma HLS stream variable=writer_burst depth=512

    hls::stream<int> writer_burst_count[N_JOIN_UNITS];
#pragma HLS stream variable=writer_burst_count depth=512

    // =========== WORK DISTRIBUTION ========== //

    // maximum number dataset B needs to be restreamed
    // e.g. if each PE gets at most 9 objects to work on, then B needs to be restreamed at most 9 times
    const int max_restreams_of_B = MAX_OBJS_PER_PAGE * std::ceil(static_cast<double>(num_pages_A) / N_JOIN_UNITS);

    int base_num_pages_of_A_per_PE = num_pages_A / N_JOIN_UNITS;
    int extra_pages_of_A = num_pages_A % N_JOIN_UNITS;

    int objects_in_last_page = num_objects_A % MAX_OBJS_PER_PAGE;
    int last_obj_PE = (num_pages_A - 1) % N_JOIN_UNITS;

    // =========== DATAFLOW ============ //

    // read A and distribute pages among PEs
    read_unit(
        dataset_A,
        num_objects_A,
        num_pages_A,
        stream_raw_A
    );

    // read B and distribute pages among PEs
    read_and_restream_unit(
        dataset_B,
        num_pages_B,

        num_objects_A,
        num_pages_A,

        stream_raw_B
    );

    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS unroll

        int num_pages_of_PE = base_num_pages_of_A_per_PE + (PE_id < extra_pages_of_A ? 1 : 0);
        int num_objects_of_PE = num_pages_of_PE * MAX_OBJS_PER_PAGE - (PE_id == last_obj_PE ? MAX_OBJS_PER_PAGE - objects_in_last_page : 0);

        // parse PE's A pages into objects
        parse_unit(
            stream_raw_A[PE_id],

            num_objects_of_PE,
            num_pages_of_PE,

            stream_parsed_A[PE_id]
        );

        // parse and reparse PE's B pages into objects
        parse_restreamed_unit(
            stream_raw_B[PE_id],
            num_objects_B,
            num_pages_B,

            num_objects_of_PE,

            stream_parsed_B[PE_id]
        );

        // join pages
        join_unit(
            // input
            num_objects_of_PE,
            stream_parsed_A[PE_id],

            num_objects_B,
            stream_parsed_B[PE_id],

            // output
            results[PE_id],
            FIN_join_for_collect[PE_id],
            FIN_join_for_write[PE_id]
        );

        // collect results into a burst buffer (FIFO pipe)
        collect_unit(
                // input
                results[PE_id],
                FIN_join_for_collect[PE_id],

                // output
                writer_burst[PE_id],
                writer_burst_count[PE_id]
        );
    }

    aggregate_signals_unit<N_JOIN_UNITS>(
            FIN_join_for_write,
            FIN_join_agg
    );

    // write results
    write_unit(
            // input
            writer_burst,
            writer_burst_count,
            FIN_join_agg,

            // output
            out_intersect
    );
}
}