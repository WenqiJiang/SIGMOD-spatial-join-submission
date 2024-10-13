#pragma once

// partitioning
#define MAX_OBJS_PER_PARTITION 100

#define PAGE_SIZE_BYTES 64

// data
#define MAX_OBJS_PER_PAGE 3
// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BYTES 20
// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BITS (OBJ_SIZE_BYTES * 8)

// meta
#define MAX_META_PER_PAGE 5
#define META_SIZE_BYTES 12 // 12 bytes
#define META_SIZE_BITS (META_SIZE_BYTES * 8)

// parallelisation
#define N_JOIN_UNITS 2

// write
#define MAX_BURSTS_PER_WRITE_REQUEST 32
// max_burst_length must be <= the output FIFO length & AXI burst length
#define MAX_BURST_LENGTH 512