#pragma once

// data

// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BYTES 20
#define PAGE_SIZE_BYTES 64
// 20 bytes (1 * id + 4 * boundary)
#define OBJ_SIZE_BITS (OBJ_SIZE_BYTES * 8)
#define MAX_OBJS_PER_PAGE 3

// meta
#define MAX_META_PER_PAGE 5
#define META_SIZE_BYTES 12
#define META_SIZE_BITS (META_BYTES * 8)

// partitioning
#define MAX_OBJS_PER_PARTITION 100