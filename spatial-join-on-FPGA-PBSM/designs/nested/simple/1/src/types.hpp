#pragma once

#include <ap_int.h>
#include <hls_stream.h>

#include "constants.hpp"


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

typedef struct {
    int num_objects_per_PE[N_JOIN_UNITS];
    int num_pages_per_PE[N_JOIN_UNITS];
    int max_num_restreams;
} config_t;