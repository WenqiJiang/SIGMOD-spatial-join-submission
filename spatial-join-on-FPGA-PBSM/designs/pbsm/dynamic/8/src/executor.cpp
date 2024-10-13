#include "types.hpp"
#include "PEs.hpp"
#include "write.hpp"

extern "C" {

void executor(
        // input

        // A
        const ap_uint<512>* dataset_A,
        const ap_uint<512>* meta_A,
        int num_objects_A,
        int num_pages_A,

        // B
        const ap_uint<512>* dataset_B,
        const ap_uint<512>* meta_B,
        int num_objects_B,
        int num_pages_B,

        int num_partitions,
        int num_pages_meta,

        // output: count of total intersections, followed by intersecting ID pairs
        hls::burst_maxi<ap_uint<64>> out_intersect
) {

#pragma HLS INTERFACE m_axi port=dataset_A offset=slave bundle=gmem0
#pragma HLS INTERFACE m_axi port=meta_A offset=slave bundle=gmem1

#pragma HLS INTERFACE m_axi port=dataset_B offset=slave bundle=gmem2
#pragma HLS INTERFACE m_axi port=meta_B offset=slave bundle=gmem3

#pragma HLS INTERFACE m_axi port=out_intersect offset=slave bundle=gmem4

#pragma HLS dataflow

    // meta

    hls::stream<ap_uint<512>> meta_stream_raw_A("meta_stream_raw_A");
#pragma HLS stream variable=meta_stream_raw_A depth=512

    hls::stream<ap_uint<512>> meta_stream_raw_B("meta_stream_raw_B");
#pragma HLS stream variable=meta_stream_raw_B depth=512

    hls::stream<partition_meta_t> meta_stream_parsed_A;
#pragma HLS stream variable=meta_stream_parsed_A depth=512

    hls::stream<partition_meta_t> meta_stream_parsed_B;
#pragma HLS stream variable=meta_stream_parsed_B depth=512

    // data

    hls::stream<ap_uint<512>> data_stream_raw_A("data_stream_raw_A");
#pragma HLS stream variable=data_stream_raw_A depth=512

    hls::stream<ap_uint<512>> data_stream_raw_B("data_stream_raw_B");
#pragma HLS stream variable=data_stream_raw_B depth=512

// scheduler

    hls::stream<ap_uint<512>> scheduled_data_stream_raw_A[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_data_stream_raw_A depth=512

    hls::stream<ap_uint<512>> scheduled_data_stream_raw_B[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_data_stream_raw_B depth=512

    hls::stream<partition_meta_t> scheduled_meta_stream_A[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_meta_stream_A depth=512

    hls::stream<partition_meta_t> scheduled_meta_stream_B[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_meta_stream_B depth=512

    hls::stream<partition_meta_t> scheduled_meta_parse_stream_A[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_meta_parse_stream_A depth=512

    hls::stream<partition_meta_t> scheduled_meta_parse_stream_B[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_meta_parse_stream_B depth=512

    hls::stream<bool> FIN_schedule_stream_A[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_schedule_stream_A depth=2

    hls::stream<bool> FIN_schedule_stream_B[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_schedule_stream_B depth=2

    hls::stream<bool> FIN_schedule_stream_join[N_JOIN_UNITS];
#pragma HLS stream variable=FIN_schedule_stream_join depth=2

    hls::stream<bool> new_partition_signal[N_JOIN_UNITS];
#pragma HLS stream variable=new_partition_signal depth=2

    // parse and join

    hls::stream<obj_t> scheduled_data_stream_parsed_A[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_data_stream_parsed_A depth=512

    hls::stream<obj_t> scheduled_data_stream_parsed_B[N_JOIN_UNITS];
#pragma HLS stream variable=scheduled_data_stream_parsed_B depth=512

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

    // dataflow

    read_unit(meta_A, num_pages_meta, meta_stream_raw_A);
    read_unit(meta_B, num_pages_meta, meta_stream_raw_B);

    read_unit(dataset_A, num_pages_A, data_stream_raw_A);
    read_unit(dataset_B, num_pages_B, data_stream_raw_B);

    parse_meta_unit(
            meta_stream_raw_A,

            num_partitions,
            num_pages_meta,

            meta_stream_parsed_A
    );

    parse_meta_unit(
            meta_stream_raw_B,

            num_partitions,
            num_pages_meta,

            meta_stream_parsed_B
    );

    schedule_unit(
            num_partitions,

            meta_stream_parsed_A,
            meta_stream_parsed_B,

            data_stream_raw_A,
            data_stream_raw_B,

            scheduled_data_stream_raw_A,
            scheduled_data_stream_raw_B,

            scheduled_meta_stream_A,
            scheduled_meta_stream_B,

            scheduled_meta_parse_stream_A,
            scheduled_meta_parse_stream_B,

            new_partition_signal,

            FIN_schedule_stream_A,
            FIN_schedule_stream_B,

            FIN_schedule_stream_join
    );

    // join units
    for (int PE_id = 0; PE_id < N_JOIN_UNITS; PE_id++) {
#pragma HLS unroll

        parse_data_unit(
                scheduled_data_stream_raw_A[PE_id],
                scheduled_meta_parse_stream_A[PE_id],
                FIN_schedule_stream_A[PE_id],
                scheduled_data_stream_parsed_A[PE_id]
        );

        parse_data_unit(
                scheduled_data_stream_raw_B[PE_id],
                scheduled_meta_parse_stream_B[PE_id],
                FIN_schedule_stream_B[PE_id],
                scheduled_data_stream_parsed_B[PE_id]
        );

        join_unit(
                // A
                scheduled_data_stream_parsed_A[PE_id],
                scheduled_meta_stream_A[PE_id],

                // B
                scheduled_data_stream_parsed_B[PE_id],
                scheduled_meta_stream_B[PE_id],

                // control
                FIN_schedule_stream_join[PE_id],
                new_partition_signal[PE_id],

                // output
                results[PE_id],
                FIN_join[PE_id]
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