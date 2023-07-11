#pragma once

#include <vector>
#include <utility>
#include "Region.h"
#include <vector>
#include <stack>
#include <omp.h>
#include <cstdio>

class Node
{
public:
  int node_id;
  int leaf;
  int count;
  MBR mbr;

  std::vector<Node *> child_ptrs;
  std::vector<int> obj_ids;
  std::vector<MBR *> mbrs;

  Node(int node_id, bool is_leaf, int size) : node_id(node_id), leaf(is_leaf),
                                              count(0)
  {
    child_ptrs.reserve(size);
    obj_ids.reserve(size);
    mbrs.reserve(size);
  }

  Node(int node_id, bool is_leaf, MBR *mbr) : node_id(node_id), leaf(is_leaf),
                                              count(0), mbr(*mbr) {}

  int get_node_id()
  {
    return node_id;
  }

  int is_leaf()
  {
    return leaf;
  }

  int get_count()
  {
    return count;
  }

  void add_entry(MBR *mbr)
  {
    mbrs.push_back(mbr);
    count++;
  }

  void add_entry(MBR *mbr, int obj_id)
  {
    mbrs.push_back(mbr);
    obj_ids.push_back(obj_id);
    count++;
  }

  void add_entry(Node *child_ptr)
  {
    mbrs.push_back(&child_ptr->mbr);
    child_ptrs.push_back(child_ptr);
    count++;
  }

  void add_entry(MBR *mbr, Node *child_ptr)
  {
    mbrs.push_back(mbr);
    child_ptrs.push_back(child_ptr);
    count++;
  }

  Node *get_child(int i) const
  {
    return child_ptrs.at(i);
  }

  void add_child(Node *child_ptr)
  {
    child_ptrs.push_back(child_ptr);
  }

  int get_obj_id(int i) const
  {
    return obj_ids.at(i);
  }

  MBR *get_mbr(int i) const
  {
    return mbrs.at(i);
  }

  void merge_mbrs(MBR *bounding)
  {
    float min0 = bounding->get_high0(), max0 = bounding->get_low0(), min1 = bounding->get_high1(), max1 = bounding->get_low1();
    for (MBR *m : mbrs)
    {
      if (m->get_low0() < min0)
        min0 = m->get_low0();
      if (m->get_low1() < min1)
        min1 = m->get_low1();
      if (m->get_high0() > max0)
        max0 = m->get_high0();
      if (m->get_high1() > max1)
        max1 = m->get_high1();
    }
    mbr = *(new MBR(min0, max0, min1, max1));
  }
};

void join_nodes_recursive(Node *node_A, Node *node_B, std::vector<std::pair<int, int>> &results)
{
  if (node_A->is_leaf() && !node_B->is_leaf())
  {
    for (int i = 0; i < node_B->get_count(); i++)
    {
      if (node_A->mbr.intersects(node_B->get_mbr(i))) {
      	join_nodes_recursive(node_A, node_B->get_child(i), results);
	  }
    }
  }
  else if (!node_A->is_leaf() && node_B->is_leaf())
  {
    for (int i = 0; i < node_A->get_count(); i++)
    {
      if (node_B->mbr.intersects(node_A->get_mbr(i))) {
        join_nodes_recursive(node_B, node_A->get_child(i), results);
	  }
    }
  }
  else if (node_A->is_leaf() && node_B->is_leaf())
  {
    for (int i = 0; i < node_A->get_count(); i++)
    {
      for (int j = 0; j < node_B->get_count(); j++)
      {
        if (node_A->get_mbr(i)->intersects(node_B->get_mbr(j)))
        {
          results.push_back({node_A->get_obj_id(i), node_B->get_obj_id(j)});
        }
      }
    }
  }
  else
  {
    for (int i = 0; i < node_A->get_count(); i++)
    {
      for (int j = 0; j < node_B->get_count(); j++)
      {
        if (node_A->get_mbr(i)->intersects(node_B->get_mbr(j)))
        {
          join_nodes_recursive(node_A->get_child(i), node_B->get_child(j), results);
        }
      }
    }
  }
}

void sync_traversal(std::vector<std::pair<int, int>> &results, Node *root_A, Node *root_B)
{
  join_nodes_recursive(root_A, root_B, results);
}

// void bfs_single(Node *root_A, Node *root_B, std::vector<std::pair<int, int>> &results)
// {
//   std::vector<std::vector<std::pair<Node *, Node *>>> queues;
//   std::vector<std::pair<Node *, Node *>> lowest_level;

//   std::pair<Node *, Node *> roots = std::make_pair(root_A, root_B);
//   lowest_level.push_back(roots);
//   queues.push_back(lowest_level);
//   bool break_loop = 0;

//   while (!break_loop)
//   {
//     std::vector<std::pair<Node *, Node *>> *curr_queue = &queues.back();
//     std::vector<std::pair<Node *, Node *>> new_level;
//     for (int i = 0; i < curr_queue->size(); i++)
//     {
//       std::pair<Node *, Node *> *curr_pair = &curr_queue->at(i);
//       Node *first = curr_pair->first;
//       Node *second = curr_pair->second;
//       int count_A = first->get_count();
//       int count_B = second->get_count();
//       if (!first->is_leaf() && !second->is_leaf())
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           Node *child_A = first->get_child(j);
//           for (int k = 0; k < count_B; k++)
//           {
//             Node *child_B = second->get_child(k);
//             if (child_A->mbr.intersects(&child_B->mbr))
//             {
//               new_level.push_back(std::make_pair(child_A, child_B));
//             }
//           }
//         }
//       }
//       else if (first->is_leaf() && !second->is_leaf())
//       {
//         for (int j = 0; j < count_B; j++)
//         {
//           Node *child_B = second->get_child(j);
//           if (first->mbr.intersects(&child_B->mbr))
//           {
//             new_level.push_back(std::make_pair(first, child_B));
//           }
//         }
//       }
//       else if (!first->is_leaf() && second->is_leaf())
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           Node *child_A = second->get_child(j);
//           if (first->mbr.intersects(&child_A->mbr))
//           {
//             new_level.push_back(std::make_pair(child_A, second));
//           }
//         }
//       }
//       else
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           MBR *mbr_A = first->get_mbr(j);
//           for (int k = 0; k < count_B; k++)
//           {
//             MBR *mbr_B = second->get_mbr(k);
//             if (mbr_A->intersects(mbr_B))
//             {
//               results.push_back({first->get_obj_id(j), second->get_obj_id(k)});
//             }
//           }
//         }
//         break_loop = 1;
//       }
//     }
//     queues.push_back(new_level);
//   }
// }

void bfs_parallel(Node *root_A, Node *root_B, std::vector<std::pair<int, int>> &results)
{
  std::vector<std::vector<std::pair<Node *, Node *>>> queues;
  std::vector<std::pair<Node *, Node *>> lowest_level;

  std::pair<Node *, Node *> roots = std::make_pair(root_A, root_B);
  lowest_level.push_back(roots);
  queues.push_back(lowest_level);
  bool break_loop = 0;

  while (!break_loop)
  {
    std::vector<std::pair<Node *, Node *>> *curr_queue = &queues.back();
    std::vector<std::pair<Node *, Node *>> new_level;
    //printf("Creating new level. Num levels: %ld\n", queues.size());

    #pragma omp parallel for schedule(runtime)
    for (int i = 0; i < curr_queue->size(); i++)
    {
      std::pair<Node *, Node *> *curr_pair = &curr_queue->at(i);
      Node *first = curr_pair->first;
      Node *second = curr_pair->second;
      int count_A = first->get_count();
      int count_B = second->get_count();

      //printf("Tasks: %ld, Level: %ld, i: %d",curr_queue->size(), queues.size(), i);

      if (!first->is_leaf() && !second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_A; j++)
        {
          Node *child_A = first->get_child(j);
          for (int k = 0; k < count_B; k++)
          {
            Node *child_B = second->get_child(k);
            if (child_A->mbr.intersects(&child_B->mbr))
            {
              new_level_local.push_back(std::make_pair(child_A, child_B));
            }
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else if (first->is_leaf() && !second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_B; j++)
        {
          Node *child_B = second->get_child(j);
          if (first->mbr.intersects(&child_B->mbr))
          {
            new_level_local.push_back(std::make_pair(first, child_B));
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else if (!first->is_leaf() && second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_A; j++)
        {
          Node *child_A = first->get_child(j);
          if (second->mbr.intersects(&child_A->mbr))
          {
            new_level_local.push_back(std::make_pair(child_A, second));
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else
      {
        std::vector<std::pair<int, int>> final_local;
        for (int j = 0; j < count_A; j++)
        {
          MBR *mbr_A = first->get_mbr(j);
          for (int k = 0; k < count_B; k++)
          {
            MBR *mbr_B = second->get_mbr(k);
            if (mbr_A->intersects(mbr_B))
            {
              final_local.push_back({first->get_obj_id(j), second->get_obj_id(k)});
            }
          }
        }
        if (!final_local.empty())
        {
          #pragma omp critical
          results.insert(results.end(), final_local.begin(), final_local.end());
        }
        break_loop = 1;
      }
    }
    #pragma omp critical
    queues.push_back(new_level);

	// std::cout << "New level size: " << new_level.size() << std::endl;
  }
}

// void bfs_dfs_single(Node *root_A, Node *root_B, std::vector<std::pair<int, int>> &results)
// {
//   // BFS part
//   std::vector<std::vector<std::pair<Node *, Node *>>> queues;
//   std::vector<std::pair<Node *, Node *>> lowest_level;

//   std::pair<Node *, Node *> roots = std::make_pair(root_A, root_B);
//   lowest_level.push_back(roots);
//   queues.push_back(lowest_level);
//   bool break_loop = 0;
//   int num_results = 0;

//   // Step 1: Join
//   // Step 2: if enough results DFS, if not keep going with BFS?
//   int th = omp_get_thread_num();
//   while (!break_loop && num_results < 10 * th);
//   {
//     std::vector<std::pair<Node *, Node *>> *curr_queue = &queues.back();
//     std::vector<std::pair<Node *, Node *>> new_level;
//     for (int i = 0; i < curr_queue->size(); i++)
//     {
//       std::pair<Node *, Node *> *curr_pair = &curr_queue->at(i);
//       Node *first = curr_pair->first;
//       Node *second = curr_pair->second;
//       int count_A = first->get_count();
//       int count_B = second->get_count();
//       if (!first->is_leaf() && !second->is_leaf())
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           Node *child_A = first->get_child(j);
//           for (int k = 0; k < count_B; k++)
//           {
//             Node *child_B = second->get_child(k);
//             if (child_A->mbr.intersects(&child_B->mbr))
//             {
//               new_level.push_back(std::make_pair(child_A, child_B));
//             }
//           }
//         }
//       }
//       else if (first->is_leaf() && !second->is_leaf())
//       {
//         for (int j = 0; j < count_B; j++)
//         {
//           Node *child_B = second->get_child(j);
//           if (first->mbr.intersects(&child_B->mbr))
//           {
//             new_level.push_back(std::make_pair(first, child_B));
//           }
//         }
//       }
//       else if (!first->is_leaf() && second->is_leaf())
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           Node *child_A = second->get_child(j);
//           if (first->mbr.intersects(&child_A->mbr))
//           {
//             new_level.push_back(std::make_pair(child_A, second));
//           }
//         }
//       }
//       else
//       {
//         for (int j = 0; j < count_A; j++)
//         {
//           MBR *mbr_A = first->get_mbr(j);
//           for (int k = 0; k < count_B; k++)
//           {
//             MBR *mbr_B = second->get_mbr(k);
//             if (mbr_A->intersects(mbr_B))
//             {
//               results.push_back({first->get_obj_id(j), second->get_obj_id(k)});
//             }
//           }
//         }
//         break_loop = 1;
//       }
//     }
//     queues.push_back(new_level);
//   }

//   std::vector<std::pair<Node *, Node *>> *intermediate = &queues.back();
//   for(int i = 0; i < intermediate->size(); i++)
//   {
//     sync_traversal(results, intermediate->at(i).first, intermediate->at(i).second);
//   }
// }

// void join_recursive_parallel(std::vector<std::pair<int, int>> &results, Node *node_A, Node *node_B)
// {
//   if (node_A->is_leaf() && !node_B->is_leaf())
//   {
//     for (int i = 0; i < node_B->get_count(); i++)
//     {
//       join_recursive_parallel(results, node_A, node_B->get_child(i));
//     }
//   }
//   else if (!node_A->is_leaf() && node_B->is_leaf())
//   {
//     for (int i = 0; i < node_A->get_count(); i++)
//     {
//       join_recursive_parallel(results, node_B, node_A->get_child(i));
//     }
//   }
//   else if (node_A->is_leaf() && node_B->is_leaf())
//   {
//     for (int i = 0; i < node_A->get_count(); i++)
//     {
//       for (int j = 0; j < node_B->get_count(); j++)
//       {
//         if (node_A->get_mbr(i)->intersects(node_B->get_mbr(j)))
//         {
//           results.push_back({node_A->get_obj_id(i), node_B->get_obj_id(j)});
//         }
//       }
//     }
//   }
//   else
//   {
//     for (int i = 0; i < node_A->get_count(); i++)
//     {
//       for (int j = 0; j < node_B->get_count(); j++)
//       {
//         if (node_A->get_mbr(i)->intersects(node_B->get_mbr(j)))
//         {
//           join_recursive_parallel(results, node_A->get_child(i), node_B->get_child(j));
//         }
//       }
//     }
//   }
// }

void bfs_dfs_parallel(Node *root_A, Node *root_B, std::vector<std::pair<int, int>> &results, int num_threads)
{
  // BFS part
  std::vector<std::vector<std::pair<Node *, Node *>>> queues;
  std::vector<std::pair<Node *, Node *>> lowest_level;

  std::pair<Node *, Node *> roots = std::make_pair(root_A, root_B);
  lowest_level.push_back(roots);
  queues.push_back(lowest_level);
  bool break_loop = 0;
  int num_results = 0;

  // Step 1: Join
  // Step 2: if enough results DFS, if not keep going with BFS?
  int threshold = 10 * num_threads;
  printf("BFS-DFS task threshold: %d\n", threshold);
  while (!(break_loop || num_results >= threshold))
  {
    std::vector<std::pair<Node *, Node *>> *curr_queue = &queues.back();
    std::vector<std::pair<Node *, Node *>> new_level;
  
    #pragma omp parallel for schedule(runtime)
    for (int i = 0; i < curr_queue->size(); i++)
    {
      std::pair<Node *, Node *> *curr_pair = &curr_queue->at(i);
      Node *first = curr_pair->first;
      Node *second = curr_pair->second;
      int count_A = first->get_count();
      int count_B = second->get_count();
      if (!first->is_leaf() && !second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_A; j++)
        {
          Node *child_A = first->get_child(j);
          for (int k = 0; k < count_B; k++)
          {
            Node *child_B = second->get_child(k);
            if (child_A->mbr.intersects(&child_B->mbr))
            {
              new_level_local.push_back(std::make_pair(child_A, child_B));
            }
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else if (first->is_leaf() && !second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_B; j++)
        {
          Node *child_B = second->get_child(j);
          if (first->mbr.intersects(&child_B->mbr))
          {
            new_level_local.push_back(std::make_pair(first, child_B));
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else if (!first->is_leaf() && second->is_leaf())
      {
        std::vector<std::pair<Node *, Node *>> new_level_local;
        for (int j = 0; j < count_A; j++)
        {
          Node *child_A = first->get_child(j);
          if (second->mbr.intersects(&child_A->mbr))
          {
            new_level_local.push_back(std::make_pair(child_A, second));
          }
        }
        if (!new_level_local.empty())
        {
          #pragma omp critical
          new_level.insert(new_level.end(), new_level_local.begin(), new_level_local.end());
        }
      }
      else
      {
        std::vector<std::pair<int, int>> final_local;
        for (int j = 0; j < count_A; j++)
        {
          MBR *mbr_A = first->get_mbr(j);
          for (int k = 0; k < count_B; k++)
          {
            MBR *mbr_B = second->get_mbr(k);
            if (mbr_A->intersects(mbr_B))
            {
              final_local.push_back({first->get_obj_id(j), second->get_obj_id(k)});
            }
          }
        }
        if (!final_local.empty())
        {
          #pragma omp critical
          results.insert(results.end(), final_local.begin(), final_local.end());
        }
        break_loop = 1;
      }
    }
    #pragma omp critical 
  {
    queues.push_back(new_level);
    num_results = new_level.size();
  }
  }
  printf("finish bfs part\n");

  // if not reaching the bottom layer, continue with dfs
  if (!break_loop) {
    std::vector<std::pair<Node *, Node *>> *intermediate = &queues.back();
    printf("continue with dfs part, num tasks:%ld\n", intermediate->size());
    #pragma omp parallel for schedule(runtime)
    for (int i = 0; i < intermediate->size(); i++)
    {
      std::vector<std::pair<int, int>> local;
      sync_traversal(local, intermediate->at(i).first, intermediate->at(i).second);
      if (!local.empty())
      {
      #pragma omp critical
      results.insert(results.end(), local.begin(), local.end());
      }
    }
  } 
}