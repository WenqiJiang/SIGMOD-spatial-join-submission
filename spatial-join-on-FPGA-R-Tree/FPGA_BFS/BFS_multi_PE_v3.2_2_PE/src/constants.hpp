#pragma once

// 1 -> point/line intersection counts as region intersection
// 0 -> does not count
#define POINT_INTERSECT_COUNTS 1 

// for meta info array in scheduler
#define MAX_TREE_LEVEL 32

// object related info
#define OBJ_BYTES 20 // 20 bytes (1 * id + 4 * boundary)
#define OBJ_BITS (OBJ_BYTES * 8) // 20 bytes (1 * id + 4 * boundary)
#define N_OBJ_PER_AXI 3 // 64 bytes can accommodate 3 objects 

// For hardware, maximum number of entries per page for a join PE
//   each obj_t will be treat as 5 arrays in HLS, each with 4 byte elements
//   	BRAM18Kbit -> 512 x int
//   this supports 512 objects * 20 bytes ~= 10 KB page size
#define MAX_PAGE_ENTRY_NUM 512 

// join PE number
#define N_JOIN_PE 2
