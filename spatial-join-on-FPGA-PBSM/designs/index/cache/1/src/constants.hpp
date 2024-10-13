#pragma once

// data
#define OBJ_SIZE_BYTES 20 // 20 bytes (1 * id + 4 * boundary)
#define PAGE_SIZE_BYTES 64
#define OBJ_SIZE_BITS (OBJ_SIZE_BYTES * 8) // 20 bytes (1 * id + 4 * boundary)
#define MAX_OBJS_PER_PAGE 3

// write
#define MAX_BURSTS_PER_WRITE_REQUEST 32
// max_burst_length must be <= the output FIFO length & AXI burst length
#define MAX_BURST_LENGTH 512

// tree
#define MAX_NODE_PAGE_NUM 32

// parallelisation
#define N_JOIN_UNITS 1