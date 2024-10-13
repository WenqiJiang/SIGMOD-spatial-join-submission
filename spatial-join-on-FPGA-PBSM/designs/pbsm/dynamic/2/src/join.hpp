#pragma once

#include <vector>
#include <algorithm>
#include <utility>
#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <chrono>
#include <cmath>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>

#include "types.hpp"
#include "utils.hpp"
#include "partition_utils.hpp"

int join_datasets(
    const std::vector<obj_t> &dataset_a,
    const std::vector<obj_t> &dataset_b
) {
    int count = 0;

    for (const obj_t& obj_A : dataset_a) {
        for (const obj_t& obj_B : dataset_b) {

            bool intersects = (obj_A.low0 <= obj_B.high0 && obj_A.high0 >= obj_B.low0) &&
                              (obj_A.low1 <= obj_B.high1 && obj_A.high1 >= obj_B.low1);

            if (intersects) {
                count++;
            }
        }
    }

    printf("Results plain join: %d\n", count);

    return count;
}


int join_grid(
        std::vector<cell_t> &grid
) {
    int count = 0;

    for (int i = 0; i < grid.size(); i++) {
        cell_t& cell = grid[i];

        for (const obj_t& obj_A : cell.objects_a) {
            for (const obj_t& obj_B : cell.objects_b) {

                bool intersects = (obj_A.low0 <= obj_B.high0 && obj_A.high0 >= obj_B.low0) &&
                                  (obj_A.low1 <= obj_B.high1 && obj_A.high1 >= obj_B.low1);

                if (intersects) {
                    float bottom_left_intersection_x = std::max(obj_A.low0, obj_B.low0);
                    float bottom_left_intersection_y = std::max(obj_A.low1, obj_B.low1);

                    bool within_cell = (bottom_left_intersection_x >= cell.min_X) &&
                                       (bottom_left_intersection_y >= cell.min_Y) &&
                                       (bottom_left_intersection_x <= cell.max_X) &&
                                       (bottom_left_intersection_y <= cell.max_Y);

                    if (within_cell) {
                        count++;
                    }
                }
            }
        }
    }

    printf("Results grid join: %d\n", count);

    return count;
}

// test C join
int join_partitions(
        std::vector<page_t, aligned_allocator<page_t>> partitions_A,
        std::vector<page_t, aligned_allocator<page_t>> meta_pages_A,
        std::vector<page_t, aligned_allocator<page_t>> partitions_B,
        std::vector<page_t, aligned_allocator<page_t>> meta_pages_B,
        int num_partitions
) {
    int count = 0;

    std::vector<partition_meta_t> meta_A;
    std::vector<partition_meta_t> meta_B;

    // unpack meta
    unpack_meta_from_pages(meta_pages_A, meta_A, num_partitions);
    unpack_meta_from_pages(meta_pages_B, meta_B, num_partitions);

    int offset_A = 0;
    int offset_B = 0;

    for (int p = 0; p < num_partitions; p++) {
        partition_meta_t partition_meta_A = meta_A[p];
        partition_meta_t partition_meta_B = meta_B[p];

        int count_A = partition_meta_A.count;
        int count_B = partition_meta_B.count;

        std::vector<obj_t> objects_A;
        std::vector<obj_t> objects_B;

        // unpack partition A
        int pages_A = (count_A + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
        for (int i = 0; i < pages_A; i++) {
            int num_objects_in_current_page;
            if (i == pages_A - 1) {
                num_objects_in_current_page = count_A - i * MAX_OBJS_PER_PAGE;
            } else {
                num_objects_in_current_page = MAX_OBJS_PER_PAGE;
            }

            unpack_page(partitions_A[offset_A + i], objects_A, num_objects_in_current_page);
        }

        // unpack partition B
        int pages_B = (count_B + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
        for (int i = 0; i < pages_B; i++) {
            int num_objects_in_current_page;
            if (i == pages_B - 1) {
                num_objects_in_current_page = count_B - i * MAX_OBJS_PER_PAGE;
            } else {
                num_objects_in_current_page = MAX_OBJS_PER_PAGE;
            }

            unpack_page(partitions_B[offset_B + i], objects_B, num_objects_in_current_page);
        }

        // join
        for (int i = 0; i < count_A; i++) {
            const obj_t& obj_A = objects_A[i];

            for (int j = 0; j < count_B; j++) {
                const obj_t& obj_B = objects_B[j];

                bool intersects = (obj_A.low0 <= obj_B.high0 && obj_A.high0 >= obj_B.low0) &&
                                  (obj_A.low1 <= obj_B.high1 && obj_A.high1 >= obj_B.low1);

                if (intersects) {
                    float bottom_left_intersection_x = std::max(obj_A.low0, obj_B.low0);
                    float bottom_left_intersection_y = std::max(obj_A.low1, obj_B.low1);

                    bool within_cell = (bottom_left_intersection_x >= partition_meta_A.x) &&
                                       (bottom_left_intersection_y >= partition_meta_A.y) &&
                                       (bottom_left_intersection_x <= partition_meta_B.x) &&
                                       (bottom_left_intersection_y <= partition_meta_B.y);

                    if (within_cell) {
                        count++;
                    }
                }
            }
        }

        offset_A += pages_A;
        offset_B += pages_B;
    }

    printf("Results partition join: %d\n", count);

    return count;
}