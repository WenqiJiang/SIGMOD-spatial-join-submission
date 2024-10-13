#pragma once

#include <ap_int.h>
#include <hls_stream.h>

#include "constants.hpp"
#include <vector>

typedef struct {
    // minimum bounding rectangle
    float low0;
    float high0;
    float low1;
    float high1;
} mbr_t;

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
    int id_A;
    int id_B;
    bool last;
} result_t;

struct partition_meta_t {
    size_t count;

    // bottom-left or top-right point
    float x;
    float y;
};

typedef struct {
    int PE;
    int count;
} task_t;