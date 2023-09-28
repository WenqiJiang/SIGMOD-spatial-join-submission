#include "1d_stripes.hpp"
#include "Tree_generation.hpp"

int main(int argc, char *argv[])
{
  FILE *trace_file;
  char *file_name_1;
  char *file_name_2;

  timespec start, end;
  std::vector<MBR *> agents;

  // simple argument parsing, not checking types, order matters
  // ./Tree-generation trace_file max_entries fill_factor
  if (argc < 4)
  {
    printf("Not enough arguments! Usage: ./Tree-generation trace_1 trace_2 tile_size\n");
    return 1;
  }
  
  file_name_1 = argv[1];
  file_name_2 = argv[2];
  int tile_size = atoi(argv[3]);
  std::cout << "Tile size: " << tile_size << std::endl;

  int numPartitions = 1;

  if (argc > 4)
  {
    printf("Too many command line arguments! Usage: ./Tree-generation trace_1 trace_2 tile_size\n");
    return 1;
  }
//   timespec start, end;

  // Read file 1
  trace_file = fopen(file_name_1, "r");
  fseek(trace_file, 0, SEEK_END);
  long long size = ftell(trace_file);
  fseek(trace_file, 0, SEEK_SET);

  char *file_buffer_1 = new char[size];
  setbuf(trace_file, file_buffer_1);

  // Parsing trace 1
  int num_rows, read;
  read = fscanf(trace_file, "%d\n", &num_rows);
  assert (num_rows >= tile_size);

  // MBR data per line
  MBR *mbr;
  float lowest0 = std::numeric_limits<float>::max();
  float highest0 = std::numeric_limits<float>::lowest();
  float lowest1 = std::numeric_limits<float>::max();
  float highest1 = std::numeric_limits<float>::lowest();
  for (int i = 0; i < tile_size; i++)
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
    Event *eventBottom = new Event(low1, true, mbr, 0);
    Event *eventTop = new Event(high1, false, mbr, 0);
    sweep_data.push_back(eventBottom);
    sweep_data.push_back(eventTop);
  }
  float l = lowest0;
  float r = highest0;
  std::vector<MBR *> R = agents; // for 1D stripe join
  Node *node_A = new Node(0, true, tile_size);
  int id_A = 0;
  for (auto mbr: agents) {
    node_A->add_entry(mbr, id_A++);
  }

  std::cout << "Finish reading file 1" << std::endl;

  // Read file 2
  trace_file = fopen(file_name_2, "r");
  fseek(trace_file, 0, SEEK_END);
  size = ftell(trace_file);
  fseek(trace_file, 0, SEEK_SET);

  char *file_buffer_2 = new char[size];
  setbuf(trace_file, file_buffer_2);

  // Parsing trace 2
  read = fscanf(trace_file, "%d\n", &num_rows);
  assert (num_rows >= tile_size);

  // MBR data per line
  agents.clear();
  lowest0 = std::numeric_limits<float>::max();
  highest0 = std::numeric_limits<float>::lowest();
  lowest1 = std::numeric_limits<float>::max();
  highest1 = std::numeric_limits<float>::lowest();
  for (int i = 0; i < tile_size; i++)
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
    if(high1 > highest1) highest1 = high1;

    mbr = new MBR(low0, high0, low1, high1); // currently only does square MBRs (what they did in VLDB'14 with non-updating indices)
    agents.push_back(mbr);
    Event *eventBottom = new Event(low1, true, mbr, 1);
    Event *eventTop = new Event(high1, false, mbr, 1);
    sweep_data.push_back(eventBottom);
    sweep_data.push_back(eventTop);
  }
  std::vector<MBR *> S = agents; // for 1D stripe join
  Node *node_B = new Node(0, true, tile_size);
  int id_B = 0;
  for (auto mbr: agents) {
    node_B->add_entry(mbr, id_B++);
  }

  std::cout << "Finish reading file 2" << std::endl;

  // Free unused memory
  fclose(trace_file);
  delete[] file_buffer_1;

  int num_iter = 1000;
//   // duplicate the input data, such that the memory access of reading the data is measured
//   std::vector<Node*> node_A_list;
//   std::vector<Node*> node_B_list;
//   for (int i = 0; i < num_iter; i++) {
// 	Node* dup_node_A = new Node(0, true, tile_size);
// 	Node* dup_node_B = new Node(0, true, tile_size);
// 	for (auto mbr: node_A->mbrs) {
// 	  MBR* dup_mbr = new MBR(mbr->get_low0(), mbr->get_high0(), mbr->get_low1(), mbr->get_high1());
// 	  dup_node_A->add_entry(dup_mbr);
// 	}
// 	for (auto mbr: node_B->mbrs) {
// 	  MBR* dup_mbr = new MBR(mbr->get_low0(), mbr->get_high0(), mbr->get_low1(), mbr->get_high1());
// 	  dup_node_B->add_entry(dup_mbr);
// 	}
// 	node_A_list.push_back(dup_node_A);
// 	node_B_list.push_back(dup_node_B);
//   }

//   std::vector<std::vector<Event *>> sweep_data_list;
//   for (int i = 0; i < num_iter; i++) {
// 	std::vector<Event *> dup_sweep_data;
// 	for (auto event: sweep_data) {
// 	  Event* dup_event = new Event(*event);
// 	  dup_sweep_data.push_back(dup_event);
// 	}
// 	sweep_data_list.push_back(dup_sweep_data);
//   }

  // Nested loop join
  std::vector<std::pair<int, int>> results_nested_loop;

  clock_gettime(CLOCK_BOOTTIME, &start);
  for (int i = 0; i < num_iter; i++) {
    results_nested_loop.clear();
    join_nodes_recursive(node_A, node_B, results_nested_loop);
    // join_nodes_recursive(node_A_list[i], node_B_list[i], results_nested_loop);
  }
  clock_gettime(CLOCK_BOOTTIME, &end);

  timespec total_time_nested_loop = diff(start, end);
  float total_time_us_nested_loop =
      ((float)total_time_nested_loop.tv_sec) * 1000.0 * 1000.0 +
      ((float)total_time_nested_loop.tv_nsec) / 1000.0;
  std::cout << "Num Results (Nested Loop): " << results_nested_loop.size() << std::endl;
  printf("Nested loop join time (per tile pair): %.2f us\n", total_time_us_nested_loop / num_iter);


  // Plane sweep join
  std::vector<std::pair<MBR *, MBR *>> results_stripe;
  clock_gettime(CLOCK_BOOTTIME, &start);
  for (int i = 0; i < num_iter; i++) {
    results_stripe.clear();
    // sweep_line_join(results_stripe, &sweep_data_list[i], l);
    sweep_line_join(results_stripe, &sweep_data, l);
  }
  clock_gettime(CLOCK_BOOTTIME, &end);

  timespec total_time_ps = diff(start, end);
  float total_time_us_ps =
      ((float)total_time_ps.tv_sec) * 1000.0 * 1000.0 +
      ((float)total_time_ps.tv_nsec) / 1000.0;
  std::cout << "Num Results (Stripe): " << results_stripe.size() << std::endl;
  printf("Plane sweep join time (per tile pair): %.2f us\n", total_time_us_ps / num_iter);

  return 0;
}