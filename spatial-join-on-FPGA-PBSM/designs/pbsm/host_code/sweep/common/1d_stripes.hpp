#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <chrono>

#include "Event.h"

typedef struct {
    int result;
    double partition_time;
    double join_time;
} run_result_t;


void sweep_line_join_dim1(std::vector<std::pair<MBR *, MBR *>> &results, std::vector<Event *> *sweep_data, float tileLow0)
{
  // Sort datasets on y -> This becomes our event queue
  std::sort(sweep_data->begin(), sweep_data->end(), []
  (const auto& lhs, const auto& rhs) {
    return *lhs < *rhs;
  });

  // Now declare active sets for each dataset: will use unordered map for efficient lookup
  std::unordered_set<MBR *> first;
  std::unordered_set<MBR *> second;

  // Algorithm is to loop over events, adding MBRs to respective set on bottom and removing on top
  // On bottom we also have to check for intersections in the respective active set and add to results
  for(const auto& ev : *sweep_data){
    if(ev->isBottom())
    {
      // add to correct active set and loop over other set
      if(ev->set()) // this means it was in second dataset
      {
        MBR *curr = ev->getMBR();
        second.insert(curr);

        for(const auto& mbr : first)
        {
          if(mbr->intersects(curr) && std::max(mbr->get_low0(), curr->get_low0()) >= tileLow0)
          {
            results.push_back(std::make_pair(curr, mbr));
          }
        }
      } else {
        MBR *curr = ev->getMBR();
        first.insert(curr);

        for(const auto& mbr : second)
        {
          if(mbr->intersects(curr) && std::max(mbr->get_low0(), curr->get_low0()) >= tileLow0)
          {
            results.push_back(std::make_pair(curr, mbr));
          }
        }
      }

    } else { // is top
      if(ev->set()) // this means it was in second dataset
      {
        second.erase(ev->getMBR());
      } else {
        first.erase(ev->getMBR());
      }
    }
  }
}

void sweep_line_join_dim0(std::vector<std::pair<MBR *, MBR *>> &results, std::vector<Event *> *sweep_data, float tileLow1)
{
  // Sort datasets on y -> This becomes our event queue
  std::sort(sweep_data->begin(), sweep_data->end(), []
  (const auto& lhs, const auto& rhs) {
    return *lhs < *rhs;
  });

  // Now declare active sets for each dataset: will use unordered map for efficient lookup
  std::unordered_set<MBR *> first;
  std::unordered_set<MBR *> second;

  // Algorithm is to loop over events, adding MBRs to respective set on bottom and removing on top
  // On bottom we also have to check for intersections in the respective active set and add to results
  for(const auto& ev : *sweep_data){
    if(ev->isBottom())
    {
      // add to correct active set and loop over other set
      if(ev->set()) // this means it was in second dataset
      {
        MBR *curr = ev->getMBR();
        second.insert(curr);

        for(const auto& mbr : first)
        {
          if(mbr->intersects(curr) && std::max(mbr->get_low1(), curr->get_low1()) >= tileLow1)
          {
            results.push_back(std::make_pair(curr, mbr));
          }
        }
      } else {
        MBR *curr = ev->getMBR();
        first.insert(curr);

        for(const auto& mbr : second)
        {
          if(mbr->intersects(curr) && std::max(mbr->get_low1(), curr->get_low1()) >= tileLow1)
          {
            results.push_back(std::make_pair(curr, mbr));
          }
        }
      }

    } else { // is top
      if(ev->set()) // this means it was in second dataset
      {
        second.erase(ev->getMBR());
      } else {
        first.erase(ev->getMBR());
      }
    }
  }
}



run_result_t stripe_join_partitoin_dim0_sweep_dim1(std::vector<std::pair<MBR *, MBR *>> &results, std::vector<MBR *> *R, std::vector<MBR *> *S,
                 float mapLow0, float mapHigh0, int numPartitions)
{
//  timespec start_partition, end_partition;
//  timespec start_join, end_join;
  float partition_side_len = (mapHigh0 - mapLow0) / numPartitions;

//  clock_gettime(CLOCK_BOOTTIME, &start_partition);
    auto start_partition = std::chrono::high_resolution_clock::now();

  // partitioning phase
  // map to store events to process for each partion: partition num -> vector of Events
  std::unordered_map<int, std::vector<Event *>> partitions;

  // initialize our vectors in map
  for(int i=0; i<numPartitions; i++)
  {
    std::vector<Event *> vec;
    partitions[i] = vec;
  }

  // do a full scan of input data, for each MBR create event and put in the right partition
  // first R
  int lowPart, highPart;
  for(const auto& mbr : *R)
  {
    lowPart = int((mbr->get_low0()-mapLow0)/partition_side_len);
    highPart = int((mbr->get_high0()-mapLow0)/partition_side_len);
	highPart = highPart >= numPartitions ? numPartitions - 1 : highPart;
    Event *eventBottom = new Event(mbr->get_low1(), true, mbr, 0);
    Event *eventTop = new Event(mbr->get_high1(), false, mbr, 0);

    for (int j = lowPart; j <= highPart; j++) {
      partitions[j].push_back(eventBottom);
      partitions[j].push_back(eventTop);
    }
  }

  // repeat for S
  for(const auto& mbr : *S)
  {
    lowPart = int((mbr->get_low0()-mapLow0)/partition_side_len);
    highPart = int((mbr->get_high0()-mapLow0)/partition_side_len);
	highPart = highPart >= numPartitions ? numPartitions - 1 : highPart;
    Event *eventBottom = new Event(mbr->get_low1(), true, mbr, 1);
    Event *eventTop = new Event(mbr->get_high1(), false, mbr, 1);

    for (int j = lowPart; j <= highPart; j++) {
      partitions[j].push_back(eventBottom);
      partitions[j].push_back(eventTop);
    }
  }
    auto end_partition = std::chrono::high_resolution_clock::now();
//  clock_gettime(CLOCK_BOOTTIME, &end_partition);

//  clock_gettime(CLOCK_BOOTTIME, &start_join);
    auto start_join = std::chrono::high_resolution_clock::now();
  // now we have our partitions and the events in them, we just join
  for (const auto& pair : partitions)
  {
    std::vector<std::pair<MBR *, MBR *>> temp;
    std::vector<Event *> cp = pair.second;
    float low = pair.first * partition_side_len + mapLow0;
	// std::cout << "partition " << pair.first << " size " << cp.size() << std::endl;

    sweep_line_join_dim1(temp, &cp, low);

    results.insert(results.end(), temp.begin(), temp.end());
  }
    auto end_join = std::chrono::high_resolution_clock::now();

    double time_partition = (std::chrono::duration_cast<std::chrono::microseconds>(end_partition - start_partition).count()) / 1000.0;
  printf("Partition the datasets (partition dim 0, sweep dim 1): %.2f ms\n", time_partition);

    double time_join = (std::chrono::duration_cast<std::chrono::microseconds>(end_join - start_join).count()) / 1000.0;
  printf("Join the datasets (partition dim 0, sweep dim 1): %.2f ms\n", time_join);

    return  (run_result_t) {
            0,
            time_partition,
            time_join
    };
}


run_result_t stripe_join_partitoin_dim1_sweep_dim0(std::vector<std::pair<MBR *, MBR *>> &results, std::vector<MBR *> *R, std::vector<MBR *> *S,
                 float mapLow1, float mapHigh1, int numPartitions)
{
//  timespec start_partition, end_partition;
//  timespec start_join, end_join;
  float partition_side_len = (mapHigh1 - mapLow1) / numPartitions;

//  clock_gettime(CLOCK_BOOTTIME, &start_partition);

    auto start_partition = std::chrono::high_resolution_clock::now();

  // partitioning phase
  // map to store events to process for each partion: partition num -> vector of Events
  std::unordered_map<int, std::vector<Event *>> partitions;

  // initialize our vectors in map
  for(int i=0; i<numPartitions; i++)
  {
    std::vector<Event *> vec;
    partitions[i] = vec;
  }

  // do a full scan of input data, for each MBR create event and put in the right partition
  // first R
  int lowPart, highPart;
  for(const auto& mbr : *R)
  {
    lowPart = int((mbr->get_low1()-mapLow1)/partition_side_len);
    highPart = int((mbr->get_high1()-mapLow1)/partition_side_len);
	highPart = highPart >= numPartitions ? numPartitions - 1 : highPart;
    Event *eventBottom = new Event(mbr->get_low0(), true, mbr, 0);
    Event *eventTop = new Event(mbr->get_high0(), false, mbr, 0);

    for (int j = lowPart; j <= highPart; j++) {
      partitions[j].push_back(eventBottom);
      partitions[j].push_back(eventTop);
    }
  }

  // repeat for S
  for(const auto& mbr : *S)
  {
    lowPart = int((mbr->get_low1()-mapLow1)/partition_side_len);
    highPart = int((mbr->get_high1()-mapLow1)/partition_side_len);
	highPart = highPart >= numPartitions ? numPartitions - 1 : highPart;
    Event *eventBottom = new Event(mbr->get_low0(), true, mbr, 1);
    Event *eventTop = new Event(mbr->get_high0(), false, mbr, 1);

    for (int j = lowPart; j <= highPart; j++) {
      partitions[j].push_back(eventBottom);
      partitions[j].push_back(eventTop);
    }
  }

    auto end_partition = std::chrono::high_resolution_clock::now();

    auto start_join = std::chrono::high_resolution_clock::now();
  // now we have our partitions and the events in them, we just join
  for (const auto& pair : partitions)
  {
    std::vector<std::pair<MBR *, MBR *>> temp;
    std::vector<Event *> cp = pair.second;
    float low = pair.first * partition_side_len + mapLow1;

    sweep_line_join_dim0(temp, &cp, low);

    results.insert(results.end(), temp.begin(), temp.end());
  }
    auto end_join = std::chrono::high_resolution_clock::now();

    double time_partition = (std::chrono::duration_cast<std::chrono::microseconds>(end_partition - start_partition).count()) / 1000.0;
  printf("Partition the datasets (partition dim 1, sweep dim 0): %.2f ms\n", time_partition);

    double time_join = (std::chrono::duration_cast<std::chrono::microseconds>(end_join - start_join).count()) / 1000.0;
  printf("Join the datasets (partition dim 1, sweep dim 0): %.2f ms\n", time_join);

    return  (run_result_t) {
        0,
            time_partition,
            time_join
    };
}