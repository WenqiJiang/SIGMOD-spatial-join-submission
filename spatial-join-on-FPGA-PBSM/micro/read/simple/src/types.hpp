#pragma once

#include <ap_int.h>
#include <hls_stream.h>
#include <cstdint>
#include <vector>
#include <array>

#include "constants.hpp"

typedef std::array<uint8_t, 64> page_t;

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
} result_t;