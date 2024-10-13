#pragma once

// partitioning
#define NUM_THREADS 16
#define MAX_OBJS_PER_PARTITION 100

// data
#define MAX_OBJS_PER_PAGE 3
#define OBJ_BYTES 20 // 20 bytes (1 * id + 4 * boundary)
#define OBJ_BITS (OBJ_BYTES * 8) // 20 bytes (1 * id + 4 * boundary)

// meta
#define MAX_META_PER_PAGE 5
#define META_BYTES 12 // 12 bytes
#define META_BITS (META_BYTES * 8)

// parallelisation
#define N_JOIN_UNITS 1

// write
#define MAX_WRITE_BURST_SIZE 32