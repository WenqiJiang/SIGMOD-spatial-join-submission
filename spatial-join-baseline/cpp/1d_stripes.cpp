#include "1d_stripes.hpp"
#include <sys/sysinfo.h>


int main(int argc, char *argv[])
{
  FILE *trace_file;
  char *file_name_1;
  char *file_name_2;

  timespec start, end;
  std::vector<MBR *> agents;

  // simple argument parsing, not checking types, order matters
  // ./Tree-generation trace_file max_entries fill_factor
  if (argc < 3)
  {
    printf("Not enough arguments! Usage: ./Tree-generation trace_1 trace_2 num_partitions(optional)\n");
    return 1;
  }
  
  file_name_1 = argv[1];
  file_name_2 = argv[2];

  int numPartitions = -1;
  if (argc > 3)
  {
    numPartitions = atoi(argv[3]);
  }

  if (argc > 4)
  {
    printf("Too many command line arguments! Usage: ./Tree-generation trace_1 trace_2 num_partitions(optional)\n");
    return 1;
  }
//   timespec start, end;

  // Read file 1
  trace_file = fopen(file_name_1, "r");
  fseek(trace_file, 0, SEEK_END);
  long long size = ftell(trace_file);
  fseek(trace_file, 0, SEEK_SET);

  // Check system memory
  struct sysinfo mem_info;
  if (sysinfo(&mem_info) != 0)
  {
    printf("Error: could not get system memory info.\n");
    return 1;
  }

  long long free_mem = mem_info.freeram * mem_info.mem_unit;
  if (free_mem < size)
  {
    printf("Error: not enough free memory to allocate buffer.\n");
    return 1;
  }

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
  for (int i = 0; i < num_rows; i++)
  {
    int id;
    float low0, high0, low1, high1;
    read = fscanf(trace_file, "%d %f %f %f %f\n", 
                &id, &low0, &high0, &low1, &high1);

    if(low0 < lowest0) lowest0 = low0;
    if(high0 > highest0) highest0 = high0;
    if(low1 < lowest1) lowest1 = low1;
    if(high1 > highest1) highest1 = high1;

    mbr = new MBR(low0, high0, low1, high1); // currently only does square MBRs (what they did in VLDB'14 with non-updating indices)
    agents.push_back(mbr);
  }
  float l = lowest0;
  float r = highest0;
  float b = lowest1;
  float t = highest1;
  std::vector<MBR *> R = agents; // for 1D stripe join


  // Read file 2
  trace_file = fopen(file_name_2, "r");
  fseek(trace_file, 0, SEEK_END);
  size = ftell(trace_file);
  fseek(trace_file, 0, SEEK_SET);

  // Check system memory
  if (sysinfo(&mem_info) != 0)
  {
    printf("Error: could not get system memory info.\n");
    return 1;
  }

  free_mem = mem_info.freeram * mem_info.mem_unit;
  if (free_mem < size)
  {
    printf("Error: not enough free memory to allocate buffer.\n");
    return 1;
  }

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
  for (int i = 0; i < num_rows; i++)
  {
    int id;
    float low0, high0, low1, high1;
    read = fscanf(trace_file, "%d %f %f %f %f\n", 
                &id, &low0, &high0, &low1, &high1);

    if(low0 < lowest0) lowest0 = low0;
    if(low0 < l) l = low0;
    if(high0 > highest0) highest0 = high0;
    if(high0 > r) r = high0;
    if(low1 < lowest1) lowest1 = low1;
    if(low1 < b) b = low1;
    if(high1 > highest1) highest1 = high1;
    if(high1 > t) t = high1;

    mbr = new MBR(low0, high0, low1, high1); // currently only does square MBRs (what they did in VLDB'14 with non-updating indices)
    agents.push_back(mbr);
  }
  std::vector<MBR *> S = agents; // for 1D stripe join

  // Free unused memory
  fclose(trace_file);
  delete[] file_buffer_1;

  std::cout << "left: " << l << " right: " << r << " bottom: " << b << " top: " << t << std::endl;

  // Perform 1d stripe sweep join
  if (numPartitions == -1)
  {
    // calculate number of partitions: here we use avg x width of MBRs, kept as 1 for now (uniform)
    // numPartitions = static_cast<int>((r - l)/10);
    std::cout << "WARNING: setting partition number by default to 1000\n";
    numPartitions = 1000; 
  }
  std::cout << "Number of partitions: " << numPartitions << std::endl;

  std::vector<std::pair<MBR *, MBR *>> stripe_results_partitoin_dim0_sweep_dim1;
  stripe_join_partitoin_dim0_sweep_dim1(stripe_results_partitoin_dim0_sweep_dim1, &R, &S, l, r, numPartitions);
  printf("Number of results (partition dim 0, sweep dim 1): %lu\n", stripe_results_partitoin_dim0_sweep_dim1.size());
  
  std::vector<std::pair<MBR *, MBR *>> stripe_results_partitoin_dim1_sweep_dim0;
  stripe_join_partitoin_dim1_sweep_dim0(stripe_results_partitoin_dim1_sweep_dim0, &R, &S, b, t, numPartitions);
  printf("Number of results (partition dim 1, sweep dim 0): %lu\n", stripe_results_partitoin_dim1_sweep_dim0.size());

  return 0;
}