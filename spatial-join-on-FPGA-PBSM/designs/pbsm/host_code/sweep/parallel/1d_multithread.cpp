#include "1d_stripes_parallel.hpp"


int main(int argc, char *argv[]) {
    FILE *trace_file;
    std::string file_name_1;
    std::string file_name_2;

    std::vector<MBR *> agents;

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " [file_name_1] [file_name_2] [numPartitions = 10] [num_threads = 1]" << std::endl;
        return EXIT_FAILURE;
    }

    file_name_1 = argv[1];
    file_name_2 = argv[2];

    int numPartitions = (argc >= 4) ? std::atoi(argv[3]) : 10;
    int num_threads = (argc >= 5) ? std::atoi(argv[4]) : 1;

    omp_set_num_threads(num_threads);

    // Read file 1
    trace_file = fopen(file_name_1.c_str(), "r");
    fseek(trace_file, 0, SEEK_END);
    long long size = ftell(trace_file);
    fseek(trace_file, 0, SEEK_SET);

    char *file_buffer_1 = new char[size];
    setbuf(trace_file, file_buffer_1);

    // Parsing trace 1
    int num_rows, read;
    read = fscanf(trace_file, "%d\n", &num_rows);

    // MBR data per line
    MBR *mbr;
    float lowest0 = std::numeric_limits<float>::max();
    float highest0 = std::numeric_limits<float>::lowest();
    float lowest1 = std::numeric_limits<float>::max();
    float highest1 = std::numeric_limits<float>::lowest();
    for (int i = 0; i < num_rows; i++) {
        int id;
        float low0, high0, low1, high1;
        read = fscanf(trace_file, "%d %f %f %f %f\n",
                      &id, &low0, &high0, &low1, &high1);

        if (low0 < lowest0) lowest0 = low0;
        if (high0 > highest0) highest0 = high0;
        if (low1 < lowest1) lowest1 = low1;
        if (high1 > highest1) highest1 = high1;

        mbr = new MBR(low0, high0, low1,
                      high1); // currently only does square MBRs (what they did in VLDB'14 with non-updating indices)
        agents.push_back(mbr);
    }
    float l = lowest0;
    float r = highest0;
    float b = lowest1;
    float t = highest1;
    std::vector<MBR *> R = agents; // for 1D stripe join

    // Read file 2
    trace_file = fopen(file_name_2.c_str(), "r");
    fseek(trace_file, 0, SEEK_END);
    size = ftell(trace_file);
    fseek(trace_file, 0, SEEK_SET);

    char *file_buffer_2 = new char[size];
    setbuf(trace_file, file_buffer_2);

    // Parsing trace 2
    read = fscanf(trace_file, "%d\n", &num_rows);

    // MBR data per line
    agents.clear();
    lowest0 = std::numeric_limits<float>::max();
    highest0 = std::numeric_limits<float>::lowest();
    lowest1 = std::numeric_limits<float>::max();
    highest1 = std::numeric_limits<float>::lowest();
    for (int i = 0; i < num_rows; i++) {
        int id;
        float low0, high0, low1, high1;
        read = fscanf(trace_file, "%d %f %f %f %f\n",
                      &id, &low0, &high0, &low1, &high1);

        if (low0 < lowest0) lowest0 = low0;
        if (low0 < l) l = low0;
        if (high0 > highest0) highest0 = high0;
        if (high0 > r) r = high0;
        if (low1 < lowest1) lowest1 = low1;
        if (low1 < b) b = low1;
        if (high1 > highest1) highest1 = high1;
        if (high1 > t) t = high1;

        mbr = new MBR(low0, high0, low1, high1); // currently only does square MBRs (what they did in VLDB'14 with non-updating indices)
        agents.push_back(mbr);
    }
    std::vector<MBR *> S = agents; // for 1D stripe join

    // Free unused memory
    fclose(trace_file);
    delete[] file_buffer_1;

    std::cout << "left: " << l << " right: " << r << " bottom: " << b << " top: " << t << std::endl;

    // 1d parallel join
    omp_set_schedule(omp_sched_dynamic, -1);
    std::cout << "Number of threads for stripe: " << omp_get_max_threads() << std::endl;
    if (numPartitions == -1) {
        // calculate number of partitions: here we use avg x width of MBRs, kept as 1 for now (uniform)
//         numPartitions = static_cast<int>((r - l)/10);
        std::cout << "WARNING: setting partition number by default to 1000\n";
        numPartitions = 1000;
    }
    std::cout << "Number of partitions: " << numPartitions << std::endl;

    std::vector<std::pair<MBR *, MBR *>> stripe_results_partitoin_dim0_sweep_dim1;
    run_result_t res_1 = stripe_join_partitoin_dim0_sweep_dim1_parallel(stripe_results_partitoin_dim0_sweep_dim1, &R, &S, l, r,
                                                   numPartitions);
    printf("Number of results (partition dim 0, sweep dim 1): %lu\n", stripe_results_partitoin_dim0_sweep_dim1.size());

    std::vector<std::pair<MBR *, MBR *>> stripe_results_partitoin_dim1_sweep_dim0;
    run_result_t res_2 = stripe_join_partitoin_dim1_sweep_dim0_parallel(stripe_results_partitoin_dim1_sweep_dim0, &R, &S, b, t,
                                                   numPartitions);
    printf("Number of results (partition dim 1, sweep dim 0): %lu\n", stripe_results_partitoin_dim1_sweep_dim0.size());

//    printf("%d, %f, %f", res_1.result, std::min(res_1.partition_time, res_2.partition_time), std::min(res_1.join_time, res_2.join_time));

    printf("TIME[cpu_partition_time]: %.2lf ms.\n", std::min(res_1.partition_time, res_2.partition_time));
    printf("TIME[cpu_join_time]: %.2lf ms.\n", std::min(res_1.join_time, res_2.join_time));
    printf("TIME[cpu_time]: %.2lf ms.\n", std::min(res_1.partition_time, res_2.partition_time) + std::min(res_1.join_time, res_2.join_time));

    return 0;
}