#pragma once

// data
#define OBJ_SIZE_BYTES 20 // 20 bytes (1 * id + 4 * boundary)
#define PAGE_SIZE_BYTES 64
#define OBJ_SIZE_BITS (OBJ_SIZE_BYTES * 8) // 20 bytes (1 * id + 4 * boundary)
#define MAX_OBJS_PER_PAGE 3