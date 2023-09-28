#pragma once

#include "Region.h"
#include "RTree.h"
#include "profiler.h"
#include <cmath>
#include <cassert>
#include <random>
#include <algorithm>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <sys/sysinfo.h>
#include <limits>
#include <unordered_set>
#include <unordered_map>
// #include <executio>

Node *root_A;
Node *root_B;
std::vector<MBR *> agents;

// STR loading parameters
int max_entries = 16;
double fill_factor = 1.0;

// Returns a pointer to an MBR that is contained within the argument MBR
MBR *random_sub_MBR(MBR &mbr)
{
  float low0 = mbr.get_low0();
  float high0 = mbr.get_high0();
  float low1 = mbr.get_low1();
  float high1 = mbr.get_high1();

  float delta_0 = high0 - low0;
  float delta_1 = high1 - low1;

  std::random_device rand_dev;
  std::mt19937 generator(rand_dev());
  std::uniform_int_distribution<int> distr(1, 100);

  std::pair<int, int> r0 = std::minmax(distr(generator), distr(generator));
  float r0_low = r0.first / 100.0;
  float r0_high = r0.second / 100.0;
  std::pair<int, int> r1 = std::minmax(distr(generator), distr(generator));
  float r1_low = r1.first / 100.0;
  float r1_high = r1.second / 100.0;

  float new_low0 = low0 + r0_low * delta_0;
  float new_high0 = high0 - r0_high * delta_0; // addition means MBR might be larger
  float new_low1 = low1 + r1_low * delta_1;
  float new_high1 = high1 - r1_high * delta_1; // addition means MBR might be larger

  MBR *sub_mbr = new MBR(new_low0, new_high0, new_low1, new_high1);
  return sub_mbr;
}

// Returns a pointer to root of a random RTree with given parameters
Node *generate_rtree(int max_level = 2, int directory_node_fanout = 2,
                     int data_node_fanout = 100, MBR root_mbr = MBR(0, 100, 0, 100))
{
  class TreeGenerator
  {
  public:
    int global_obj_counter;
    int global_nodes_counter;

    TreeGenerator(int global_nodes_counter = 1)
    {
      this->global_obj_counter = 0;
      this->global_nodes_counter = global_nodes_counter;
    }

    void generate_recursive(Node &node, int current_level, int max_level,
                            int directory_node_fanout, int data_node_fanout)
    {
      if (current_level == max_level)
      {
        for (int i = 0; i < data_node_fanout; i++)
        {
          node.add_entry(random_sub_MBR(node.mbr), this->global_obj_counter);
          this->global_obj_counter++;
        }
      }
      else
      {
        for (int i = 0; i < directory_node_fanout; i++)
        {
          MBR *child_mbr = random_sub_MBR(node.mbr);
          Node *child_node;
          if (current_level == max_level - 1)
          {
            child_node = new Node(this->global_nodes_counter, true, child_mbr);
          }
          else
          {
            child_node = new Node(this->global_nodes_counter, false, child_mbr);
          }
          this->global_nodes_counter++;
          node.add_entry(child_mbr, &(*child_node));
          this->generate_recursive(*child_node, current_level + 1, max_level,
                                   directory_node_fanout, data_node_fanout);
        }
      }
    }
  };

  bool root_is_leaf = (max_level == 1);
  Node *root_node = new Node(0, root_is_leaf, &root_mbr);
  TreeGenerator tree_generator(1);
  tree_generator.generate_recursive(*root_node, 1, max_level,
                                    directory_node_fanout, data_node_fanout);

  return root_node;
}

// Function to iterate over an RTree from root and write references into a vector of nodes node_list
// nodes are ordered by their node_id (i.e level order tree traversal)
void collect_all_nodes(std::vector<Node *> &node_list, Node *root)
{
  std::vector<Node *> candidate_node_list;

  candidate_node_list.push_back(root);
  while (!candidate_node_list.empty())
  {
    Node *current_node = candidate_node_list.front();
    candidate_node_list.erase(candidate_node_list.begin());
    node_list.push_back(current_node);
    if (!current_node->is_leaf())
    {
      auto cnt = current_node->get_count();
      for (int i = 0; i < cnt; i++)
      {
        auto child = current_node->get_child(i);
        candidate_node_list.push_back(child);
      }
    }
  }

  std::sort(node_list.begin(), node_list.end(), [](Node *node_A, Node *node_B)
            { return node_A->get_node_id() < node_B->get_node_id(); });
}

// Function to serialize node into buffer, here we follow the structure defined in
// method index_serialization of Index/Tree_generation.py
void node_serialization(char *buffer, uint32_t node_bytes, Node *node)
{
  uint8_t empty_byte = 0;
  uint32_t AXI_bytes = 64;
  uint32_t obj_bytes = 20;
  uint32_t header_bytes = 28;

  uint8_t leaf_bytes[4] = {0};
  leaf_bytes[0] = node->is_leaf() ? (uint8_t)1 : (uint8_t)0;

  // Serializing node variables
  unsigned int offset = 0;
  memcpy(buffer, leaf_bytes, sizeof(leaf_bytes));
  offset += sizeof(leaf_bytes);

  int count = node->get_count();
  memcpy(buffer + offset, &count, sizeof(count));
  offset += sizeof(count);

  int node_id = node->get_node_id();
  memcpy(buffer + offset, &node_id, sizeof(node_id));
  offset += sizeof(node_id);

  // Serializing node MBR
  float l0 = node->mbr.get_low0();
  float h0 = node->mbr.get_high0();
  float l1 = node->mbr.get_low1();
  float h1 = node->mbr.get_high1();
  memcpy(buffer + offset, &l0, sizeof(l0));
  offset += sizeof(l0);
  memcpy(buffer + offset, &h0, sizeof(h0));
  offset += sizeof(h0);
  memcpy(buffer + offset, &l1, sizeof(l1));
  offset += sizeof(l1);
  memcpy(buffer + offset, &h1, sizeof(h1));
  offset += sizeof(h1);

  // Padding node header
  uint8_t header_pad[AXI_bytes - header_bytes] = {0};
  memcpy(buffer + offset, header_pad, sizeof(header_pad));
  offset += sizeof(header_pad);

  // Serialize MBRs of a leaf node
  if (node->is_leaf())
  {
    for (int i = 0; i < std::ceil(count / 3.0); i++)
    {
      for (int j = 0; j < 3; j++)
      {
        int child_id = i * 3 + j;
        if (child_id >= count)
          break;
        int obj_id = node->get_obj_id(child_id);
        memcpy(buffer + offset, &obj_id, sizeof(obj_id));
        offset += sizeof(obj_id);
        // mbrs
        l0 = node->get_mbr(child_id)->get_low0();
        h0 = node->get_mbr(child_id)->get_high0();
        l1 = node->get_mbr(child_id)->get_low1();
        h1 = node->get_mbr(child_id)->get_high1();
        memcpy(buffer + offset, &l0, sizeof(l0));
        offset += sizeof(l0);
        memcpy(buffer + offset, &h0, sizeof(h0));
        offset += sizeof(h0);
        memcpy(buffer + offset, &l1, sizeof(l1));
        offset += sizeof(l1);
        memcpy(buffer + offset, &h1, sizeof(h1));
        offset += sizeof(h1);
      }
      uint8_t object_pad[AXI_bytes - 3 * obj_bytes] = {0};
      memcpy(buffer + offset, object_pad, sizeof(object_pad));
      offset += sizeof(object_pad);
    }
  }
  // Otherwise node is a directory, serialize children refs
  else
  {
    for (int i = 0; i < std::ceil(count / 3.0); i++)
    {
      for (int j = 0; j < 3; j++)
      {
        int child_id = i * 3 + j;
        if (child_id >= count)
          break;
        int child_node_id = node->get_child(child_id)->get_node_id();
        memcpy(buffer + offset, &child_node_id, sizeof(child_node_id));
        offset += sizeof(child_node_id);
        // mbrs
        l0 = node->get_child(child_id)->mbr.get_low0();
        h0 = node->get_child(child_id)->mbr.get_high0();
        l1 = node->get_child(child_id)->mbr.get_low1();
        h1 = node->get_child(child_id)->mbr.get_high1();
        memcpy(buffer + offset, &l0, sizeof(l0));
        offset += sizeof(l0);
        memcpy(buffer + offset, &h0, sizeof(h0));
        offset += sizeof(h0);
        memcpy(buffer + offset, &l1, sizeof(l1));
        offset += sizeof(l1);
        memcpy(buffer + offset, &h1, sizeof(h1));
        offset += sizeof(h1);
      }
      uint8_t object_pad[AXI_bytes - 3 * obj_bytes] = {0};
      memcpy(buffer + offset, object_pad, sizeof(object_pad));
      offset += sizeof(object_pad);
    }
  }

  // Final padding up to page size
  if (offset < node_bytes)
  {
    uint8_t pad[node_bytes - offset] = {0};
    memcpy(buffer + offset, pad, sizeof(pad));
    offset += sizeof(pad);
  }
}

// Function to serialize RTree rooted at root in file file_name
void index_serialization(Node *root, char *file_name, int max_entries)
{
  std::vector<Node *> node_list;
  collect_all_nodes(node_list, root);
  int num_nodes = node_list.size();

  std::ofstream file(file_name, std::ios::out | std::ios::binary);
  if (!file)
  {
    throw std::runtime_error("Cannot open file.");
  }
  int header_bytes = 64;
  int data_bytes = max_entries % 3 == 0? (max_entries / 3) * 64 : (max_entries / 3 + 1) * 64;
  int node_bytes = header_bytes + data_bytes;
  std::cout << "Bytes per node during serialization: " << node_bytes << std::endl;

  for (int i = 0; i < num_nodes; i++)
  {
    //printf("\nCurrent node id: %d", node_list.at(i)->get_node_id());
    char *buffer = new char[node_bytes];
    node_serialization(buffer, node_bytes, node_list.at(i));
    file.write(buffer, node_bytes);
    delete[] buffer;
  }

  file.close();
}

// Function returns pointer to a new node created by deserializing buffer node_bin
Node *node_deserialization(char *node_bin)
{
  // Deserializing node header
  unsigned int offset = 0;
  int is_leaf = *(int *)(node_bin);
  offset += sizeof(is_leaf);
  int count = *(int *)(node_bin + offset);
  offset += sizeof(count);
  int node_id = *(int *)(node_bin + offset);
  offset += sizeof(node_id);

  // Deserializing node MBR
  float low0 = *(float *)(node_bin + offset);
  offset += sizeof(low0);
  float high0 = *(float *)(node_bin + offset);
  offset += sizeof(high0);
  float low1 = *(float *)(node_bin + offset);
  offset += sizeof(low1);
  float high1 = *(float *)(node_bin + offset);
  offset += sizeof(high1);

  MBR *mbr = new MBR(low0, high0, low1, high1);
  Node *node = new Node(node_id, is_leaf, mbr);

  // Deserilizing node children
  for (int i = 0; i < std::ceil(count / 3.0); i++)
  {
    for (int j = 0; j < 3; j++)
    {
      offset = 0;
      int child_id = i * 3 + j;
      int child_id_addr = 64 + 64 * i + 20 * j;
      if (child_id >= count)
        break;
      int id = *(int *)(node_bin + offset + child_id_addr);
      offset += sizeof(id);
      low0 = *(float *)(node_bin + offset + child_id_addr);
      offset += sizeof(low0);
      high0 = *(float *)(node_bin + offset + child_id_addr);
      offset += sizeof(high0);
      low1 = *(float *)(node_bin + offset + child_id_addr);
      offset += sizeof(low1);
      high1 = *(float *)(node_bin + offset + child_id_addr);
      offset += sizeof(high1);

      MBR *mbr = new MBR(low0, high0, low1, high1);
      node->add_entry(mbr, id);
    }
  }

  return node;
}

// Function to return the root of an RTree by deserializing file file_name
Node *load_serialized_index(char *file_name)
{
  std::ifstream file(file_name, std::ios::binary);
  if (!file)
  {
    throw std::runtime_error("Cannot open file.");
  }
  file.seekg(0, std::ios::end);
  std::streampos file_size = file.tellg();

  // Check system memory
  struct sysinfo mem_info;
  if (sysinfo(&mem_info) != 0)
  {
    printf("Error: could not get system memory info.\n");
    exit(1);
  }

  long long free_mem = mem_info.freeram * mem_info.mem_unit;
  if (free_mem < file_size)
  {
    printf("Error: not enough free memory to allocate buffer.\n");
    exit(1);
  }

  char *buffer = new char[file_size];
  file.seekg(0, std::ios::beg);
  file.read(buffer, file_size);
  file.close();

  int num_nodes = file_size / 4096;
  std::vector<Node *> node_list;
  for (int i = 0; i < num_nodes; i++)
  {
    char *node_bin = buffer + i * 4096;
    Node *node = node_deserialization(node_bin);
    node_list.push_back(node);
  }

  delete[] buffer;

  std::sort(node_list.begin(), node_list.end(), [](Node *node_A, Node *node_B)
            { return node_A->get_node_id() < node_B->get_node_id(); });

  // children pointers
  for (Node *node : node_list)
  {
    if (!node->is_leaf())
    {
      for (int i = 0; i < node->get_count(); ++i)
      {
        int child_id = node->get_obj_id(i);
        node->add_child(node_list.at(child_id));
      }
    }
  }

  Node *root = node_list.at(0);

  return root;
}

// Comparison functions needed for STR bulk-loading
bool cmp_x(MBR *mbr_A, MBR *mbr_B)
{
  return mbr_A->get_low0() < mbr_B->get_low0();
}

bool cmp_y(MBR *mbr_A, MBR *mbr_B)
{
  return mbr_A->get_low1() < mbr_B->get_low1();
}

// Function that fixes node member variables for a STR-loaded RTree using a level order traversal
void fix_tree(Node *root)
{
  int id_counter = 0;

  // Queue to keep track of unexplored nodes
  std::queue<Node *> nodes;
  nodes.push(root);
  while (!nodes.empty())
  {
    Node *curr = nodes.front();
    // fix node_id
    curr->node_id = id_counter;
    id_counter++;
    // // fix mbr
    // curr->merge_mbrs(10000);
    nodes.pop();

    if (!curr->leaf)
    {
      for (int i = 0; i < curr->count; i++)
      {
        nodes.push(curr->get_child(i));
      }
    }
  }
}

// Functions build and bulk_load build an RTree for given MBRs using the STR bulk loading strategy
// build is a recursive function that builds the tree bottom-up, result in root
// functions inspired from VLDB'14 repo
void build(std::vector<Node *> &internalNodes, int tree, MBR *bounding)
{
  // if only 1 node left then that is root
  if (internalNodes.size() == 1)
  {
    if (tree == 0)
      root_A = internalNodes.at(0);
    else if (tree == 1)
      root_B = internalNodes.at(0);
  }
  else
  {
    std::vector<Node *> tempNodes;
    uint32_t numInNode = (uint32_t)(fill_factor * max_entries);
    int i;

    // build internal node based on lower level
    bool executed = false;
    for (i = 0; i < (int)internalNodes.size() - (int)numInNode; i += numInNode)
    {
      executed = true;
      Node *inode = new Node(-1, false, max_entries);

      for (int k = i; k < i + numInNode; k++)
      {
        inode->add_entry(internalNodes.at(k));
      }
      inode->merge_mbrs(bounding);
      tempNodes.push_back(inode);
    }

    bool added = false;
    Node *inode = new Node(-1, false, max_entries);
    int rem;
    if(executed)
    {
      rem = internalNodes.size() % numInNode;
      if(rem==0)
        rem = numInNode;
    }
    else
    {
      rem = internalNodes.size();
    }
    // internal node for remainder that is not multiple of numInNode
    for (int k = internalNodes.size() - rem; k < internalNodes.size(); k++)
    {
      inode->add_entry(internalNodes.at(k));
      added = true;
    }

    if (added)
    {
      inode->merge_mbrs(bounding);
      tempNodes.push_back(inode);
    }
    else
    {
      delete inode;
    }

    // recursively build higher level
    build(tempNodes, tree, bounding);
  }
}

// void build_parallel(std::vector<Node *> &internalNodes, int tree, MBR *bounding)
// {
//   // if only 1 node left then that is root
//   if (internalNodes.size() == 1)
//   {
//     if (tree == 0)
//       root_A = internalNodes.at(0);
//     else if (tree == 1)
//       root_B = internalNodes.at(0);
//   }
//   else
//   {
//     std::vector<Node *> tempNodes;
//     uint32_t numInNode = (uint32_t)(fill_factor * max_entries);
//     int i;

//     // build internal node based on lower level
//     bool executed = false;
//     #pragma omp parallel for ordered
//     for (i = 0; i < (int)internalNodes.size() - (int)numInNode; i += numInNode)
//     {
//       executed = true;
//       Node *inode = new Node(-1, false, max_entries);

//       for (int k = i; k < i + numInNode; k++)
//       {
//         inode->add_entry(internalNodes.at(k));
//       }
//       inode->merge_mbrs(bounding);

//       #pragma omp ordered
//       tempNodes.push_back(inode);
//     }

//     bool added = false;
//     Node *inode = new Node(-1, false, max_entries);
//     int rem;
//     if(executed)
//     {
//       rem = internalNodes.size() % numInNode;
//       if(rem==0)
//         rem = numInNode;
//     }
//     else
//     {
//       rem = internalNodes.size();
//     }
//     // internal node for remainder that is not multiple of numInNode
//     for (int k = internalNodes.size() - rem; k < internalNodes.size(); k++)
//     {
//       inode->add_entry(internalNodes.at(k));
//       added = true;
//     }

//     if (added)
//     {
//       inode->merge_mbrs(bounding);
//       tempNodes.push_back(inode);
//     }
//     else
//     {
//       delete inode;
//     }

//     // recursively build higher level
//     build_parallel(tempNodes, tree, bounding);
//   }
// }

void bulk_load(std::vector<MBR *> *agents, int tree, MBR *bounding)
{
  uint32_t numInLeaf = (uint32_t)(fill_factor * max_entries);
  uint32_t P = ceil((double)(agents->size()) / numInLeaf);
  uint32_t S = sqrt(P);

  // sort the points by x-coordinates
  std::sort(agents->begin(), agents->end(), cmp_x);

  // roughly number of points in a slice
  uint32_t Sn = S * numInLeaf;

  // sort agents in each slice by y-coordinate.
  int i;
  for (i = 0; i < (int)agents->size() - (int)Sn; i += Sn)
  {
    std::sort(agents->begin() + i, agents->begin() + i + Sn, cmp_y);
  }

  std::sort(agents->begin() + i, agents->end(), cmp_y);

  std::vector<Node *> tempNodes;
  // build all the leaf nodes
  for (i = 0; i < (int)agents->size() - (int)numInLeaf; i += numInLeaf)
  {
    Node *leaf = new Node(-1, true, max_entries);

    for (int k = i; k < i + numInLeaf; k++)
    {
      leaf->add_entry(agents->at(k), k);
    }
    leaf->merge_mbrs(bounding);
    tempNodes.push_back(leaf);
  }

  // remainder of leaf nodes
  Node *leaf = new Node(-1, true, max_entries);
  bool added = false;
  for (int k = (int)agents->size() - (int)numInLeaf; k < agents->size(); k++)
  {
    leaf->add_entry(agents->at(k), k);
    added = true;
  }

  if (added)
  {
    leaf->merge_mbrs(bounding);
    tempNodes.push_back(leaf);
  }
  else
  {
    delete leaf;
  }

  build(tempNodes, tree, bounding);
}

// void bulk_load_parallel(std::vector<MBR *> *agents, int tree, MBR *bounding)
// {
//   uint32_t numInLeaf = (uint32_t)(fill_factor * max_entries);
//   uint32_t P = ceil((double)(agents->size()) / numInLeaf);
//   uint32_t S = sqrt(P);

//   // sort the points by x-coordinates
//   std::sort(std::execution::par_unseq, agents->begin(), agents->end(), cmp_x);

//   // roughly number of points in a slice
//   uint32_t Sn = S * numInLeaf;

//   // sort agents in each slice by y-coordinate.
//   int i;
//   for (i = 0; i < (int)agents->size() - (int)Sn; i += Sn)
//   {
//     std::sort(std::execution::par_unseq, agents->begin() + i, agents->begin() + i + Sn, cmp_y);
//   }

//   std::sort(std::execution::par_unseq, agents->begin() + i, agents->end(), cmp_y);

//   std::vector<Node *> tempNodes;
//   // build all the leaf nodes
//   #pragma omp parallel for ordered
//   for (i = 0; i < (int)agents->size() - (int)numInLeaf; i += numInLeaf)
//   {
//     Node *leaf = new Node(-1, true, max_entries);

//     for (int k = i; k < i + numInLeaf; k++)
//     {
//       leaf->add_entry(agents->at(k), k);
//     }
//     leaf->merge_mbrs(bounding);

//     #pragma omp ordered
//     tempNodes.push_back(leaf);
//   }

//   // remainder of leaf nodes
//   Node *leaf = new Node(-1, true, max_entries);
//   bool added = false;
//   for (int k = (int)agents->size() - (int)numInLeaf; k < agents->size(); k++)
//   {
//     leaf->add_entry(agents->at(k), k);
//     added = true;
//   }

//   if (added)
//   {
//     leaf->merge_mbrs(bounding);
//     tempNodes.push_back(leaf);
//   }
//   else
//   {
//     delete leaf;
//   }

//   build_parallel(tempNodes, tree, bounding);
// }

// Naive slow nested-loop join to compare with results from using RTree index
void nested_loop_join(std::vector<std::pair<int, int>> &results)
{
  int len = agents.size();
  for (int i = 0; i < len; i++)
  {
    for (int j = 0; j < len; j++)
    {
      if (agents.at(i)->intersects(agents.at(j)))
        results.push_back({i, j});
    }
  }
}
