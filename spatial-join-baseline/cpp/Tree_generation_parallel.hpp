#pragma once

#include "Tree_generation.hpp"
#include <execution>


void build_parallel(std::vector<Node *> &internalNodes, int tree, MBR *bounding)
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
    #pragma omp parallel for ordered
    for (i = 0; i < (int)internalNodes.size() - (int)numInNode; i += numInNode)
    {
      executed = true;
      Node *inode = new Node(-1, false, max_entries);

      for (int k = i; k < i + numInNode; k++)
      {
        inode->add_entry(internalNodes.at(k));
      }
      inode->merge_mbrs(bounding);

      #pragma omp ordered
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
    build_parallel(tempNodes, tree, bounding);
  }
}


void bulk_load_parallel(std::vector<MBR *> *agents, int tree, MBR *bounding)
{
  uint32_t numInLeaf = (uint32_t)(fill_factor * max_entries);
  uint32_t P = ceil((double)(agents->size()) / numInLeaf);
  uint32_t S = sqrt(P);

  // sort the points by x-coordinates
  std::sort(std::execution::par_unseq, agents->begin(), agents->end(), cmp_x);

  // roughly number of points in a slice
  uint32_t Sn = S * numInLeaf;

  // sort agents in each slice by y-coordinate.
  int i;
  for (i = 0; i < (int)agents->size() - (int)Sn; i += Sn)
  {
    std::sort(std::execution::par_unseq, agents->begin() + i, agents->begin() + i + Sn, cmp_y);
  }

  std::sort(std::execution::par_unseq, agents->begin() + i, agents->end(), cmp_y);

  std::vector<Node *> tempNodes;
  // build all the leaf nodes
  #pragma omp parallel for ordered
  for (i = 0; i < (int)agents->size() - (int)numInLeaf; i += numInLeaf)
  {
    Node *leaf = new Node(-1, true, max_entries);

    for (int k = i; k < i + numInLeaf; k++)
    {
      leaf->add_entry(agents->at(k), k);
    }
    leaf->merge_mbrs(bounding);

    #pragma omp ordered
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

  build_parallel(tempNodes, tree, bounding);
}
