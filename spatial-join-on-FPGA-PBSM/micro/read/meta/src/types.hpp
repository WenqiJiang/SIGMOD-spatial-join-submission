#pragma once

#include <ap_int.h>
#include <hls_stream.h>
#include <vector>
#include <cstdint>
#include <iostream>
#include <vector>
#include <fstream>
#include <ap_int.h>
#include <array>

#include "constants.hpp"

typedef std::array<uint8_t, 64> page_t;

typedef struct {
    // obj id for data nodes; pointer to children for directory nodes
    int id; 
    // minimum bounding rectangle 
    float low0; 
    float high0; 
    float low1; 
    float high1; 
} obj_t;

typedef struct {
    // these IDs can either be object IDs (for data nodes)
    //   or pointer to the children (directory nodes)
    int id_A;
    int id_B;
} pair_t;

typedef struct {
    // for the last iteration, the content of pair does not count
    //   only read the 'last'=true argument
    pair_t pair;
    bool last;    // whether this is the last iteration of result sending
} result_t;

struct partition_meta_t {
    int count;

    // bottom-left or top-right point
    float x;
    float y;
};

typedef struct {
    int PE;
    int count;
} task_t;