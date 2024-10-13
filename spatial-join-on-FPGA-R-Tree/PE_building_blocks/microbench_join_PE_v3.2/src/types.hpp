#pragma once

#include <ap_int.h>
#include <hls_stream.h>

#include "constants.hpp"

//////////////////////////////////
//   Tree format on FPGAs       //
//////////////////////////////////
// Node distribution:
//   [node 0, node 1, ... node N]
//   each node has a size of page_bytes (e.g., 4 KB)
//   the address of each node can be induced by node_id * page_bytes
// 
// Within a node:
//   first 64 byte: meta data (node_meta_t), contains the basic info -
//     is this a leaf? children count? node id? node MBR? 
//   the rest of the page consists of children:
//     within each 64-byte block, 60 bytes are actually used, which 
//       contains 3 * obj_t
//     if the node is a leaf, then the id in obj_t is object id, 
//       otherwise it is a child node id
//////////////////////////////////

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
    // 7 * 4 bytes = 28 bytes
    int is_leaf;  // bool 
    int count;    // valid items
    obj_t obj;    // id/ptr + mbr
} node_meta_t;

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

typedef struct {
    // starting from the given address, read/write num times
    ap_uint<64> addr; 
    int num;
} mem_burst_t; 