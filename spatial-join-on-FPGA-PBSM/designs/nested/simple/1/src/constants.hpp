#pragma once

// page

// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BYTES 20
#define PAGE_SIZE_BYTES 64
// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BITS (OBJ_SIZE_BYTES * 8)
#define MAX_OBJS_PER_PAGE 3

// write
#define MAX_BURSTS_PER_WRITE_REQUEST 32
#define MAX_BURST_LENGTH 512

// parallelisation
#define N_JOIN_UNITS 1