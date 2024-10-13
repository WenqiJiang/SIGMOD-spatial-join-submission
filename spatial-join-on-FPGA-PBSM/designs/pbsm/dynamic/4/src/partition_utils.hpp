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
#include <random>

#include "types.hpp"
#include "utils.hpp"

typedef std::array<uint8_t, 64> page_t;

struct map_bounds_t {
    float min_x;
    float max_x;
    float min_y;
    float max_y;
};

struct map_stats_t {
    float min_cell_size_x;
    float min_cell_size_y;
};

struct partition_stats_t {
    int total_objects;
    double average_objects;
    size_t max_objects;
};

struct prepare_partitions_result_t {
    int num_partitions;
    partition_stats_t stats_A;
    partition_stats_t stats_B;
};

struct host_partition_result_t {
    std::vector<page_t, aligned_allocator<page_t>> partitions_A;
    std::vector<page_t, aligned_allocator<page_t>> meta_A;
    std::vector<page_t, aligned_allocator<page_t>> partitions_B;
    std::vector<page_t, aligned_allocator<page_t>> meta_B;

    prepare_partitions_result_t stats;

    double init_grid_time;
    double partition_objects_time;
    double refine_partitions_time;
    double prepare_partitions_time;
};

struct cell_t {
    std::vector<obj_t> objects_a;
    std::vector<obj_t> objects_b;

    float min_X, max_X, min_Y, max_Y;
};


// 1. INIT GRID

// map extremes
map_bounds_t get_map_bounds(
        const std::vector<obj_t> &objects_A,
        const std::vector<obj_t> &objects_B
) {
    float map_min_x = std::numeric_limits<float>::max();
    float map_max_x = std::numeric_limits<float>::lowest();

    float map_min_y = std::numeric_limits<float>::max();
    float map_max_y = std::numeric_limits<float>::lowest();

    // low_0 and high_0 of the map
    for (const auto &object: objects_A) {
        // x
        if (object.low0 < map_min_x) {
            map_min_x = object.low0;
        }

        if (object.high0 > map_max_x) {
            map_max_x = object.high0;
        }

        // y
        if (object.low1 < map_min_y) {
            map_min_y = object.low1;
        }

        if (object.high1 > map_max_y) {
            map_max_y = object.high1;
        }
    }

    for (const auto &object: objects_B) {
        // x
        if (object.low0 < map_min_x) {
            map_min_x = object.low0;
        }

        if (object.high0 > map_max_x) {
            map_max_x = object.high0;
        }

        // y
        if (object.low1 < map_min_y) {
            map_min_y = object.low1;
        }

        if (object.high1 > map_max_y) {
            map_max_y = object.high1;
        }
    }

    return { map_min_x, map_max_x, map_min_y, map_max_y };
}

std::vector<cell_t> init_grid_1D(
        int num_partitions,
        map_bounds_t &map_bounds,
        float cell_width,
        float cell_height
) {
    std::vector<cell_t> grid(num_partitions * num_partitions);

    for (int i = 0; i < num_partitions; i++) {
        for (int j = 0; j < num_partitions; j++) {
            int index = i * num_partitions + j;

            grid[index].min_X = map_bounds.min_x + j * cell_width;
            grid[index].max_X = grid[index].min_X + cell_width;
            grid[index].min_Y = map_bounds.min_y + i * cell_height;
            grid[index].max_Y = grid[index].min_Y + cell_height;

        }
    }

    return grid;
}

void visualise_grid_1D_text(const std::vector<cell_t>& grid, int num_partitions) {
    for (int i = 0; i < num_partitions; i++) {
        for (int j = 0; j < num_partitions; j++) {
            int index = i * num_partitions + j;
            std::cout << "[" << std::setw(4) << index << "] ";
        }

        std::cout << "\n";
    }
    std::cout << std::endl;
}


//map_stats_t compute_map_stats(
//        std::vector<obj_t>& dataset_a,
//        std::vector<obj_t>& dataset_b
//) {
//    // collect
//    std::vector<float> widths;
//    std::vector<float> heights;
//
//    for (const auto& obj : dataset_a) {
//        float width = obj.high0 - obj.low0;
//        float height = obj.high1 - obj.low1;
//        widths.push_back(width);
//        heights.push_back(height);
//    }
//
//    for (const auto& obj : dataset_b) {
//        float width = obj.high0 - obj.low0;
//        float height = obj.high1 - obj.low1;
//        widths.push_back(width);
//        heights.push_back(height);
//    }
//
//    // sort
//    std::sort(widths.begin(), widths.end());
//    std::sort(heights.begin(), heights.end());
//
//    // median
//    float median_width = widths[widths.size() / 2];
//    float median_height = heights[heights.size() / 2];
//
//    return { median_width, median_height };
//}


std::vector<obj_t> sample_data(const std::vector<obj_t>& dataset, size_t sample_size) {
    std::vector<obj_t> sample;

    std::sample(
            dataset.begin(),
            dataset.end(),
            std::back_inserter(sample),
            sample_size,
            std::mt19937{std::random_device{}()}
    );

    return sample;
}

size_t determine_sample_size(size_t dataset_size) {
    if (dataset_size <= 10000) {
        // small -> full dataset
        return dataset_size;
    } else if (dataset_size <= 50000) {
        // medium -> 5%
        return dataset_size * 0.05;
    } else if (dataset_size <= 100000) {
        // large -> 2%
        return dataset_size * 0.02;
    } else {
        // larger -> 1% or max 5000
        return std::min<size_t>(5000, dataset_size * 0.01);
    }
}

map_stats_t compute_map_stats(
        std::vector<obj_t>& dataset_a,
        std::vector<obj_t>& dataset_b
) {
    // determine sample sizes
    size_t sample_size_a = determine_sample_size(dataset_a.size());
    size_t sample_size_b = determine_sample_size(dataset_b.size());

    // sample A and B
    std::vector<obj_t> sample_a = sample_data(dataset_a, sample_size_a);
    std::vector<obj_t> sample_b = sample_data(dataset_b, sample_size_b);

    // collect
    std::vector<float> widths;
    std::vector<float> heights;

    for (const auto& obj : sample_a) {
        float width = obj.high0 - obj.low0;
        float height = obj.high1 - obj.low1;
        widths.push_back(width);
        heights.push_back(height);
    }

    for (const auto& obj : sample_b) {
        float width = obj.high0 - obj.low0;
        float height = obj.high1 - obj.low1;
        widths.push_back(width);
        heights.push_back(height);
    }

    // sort
    std::sort(widths.begin(), widths.end());
    std::sort(heights.begin(), heights.end());

    // median
    float median_width = widths[widths.size() / 2];
    float median_height = heights[heights.size() / 2];

//    // min
//    float smallest_width = widths.front();
//    float smallest_height = heights.front();
//
////     minimum cell size
//    float min_cell_size_x = std::max(smallest_width, median_width * 0.5f);
//    float min_cell_size_y = std::max(smallest_height, median_height * 0.5f);
//
//    return { min_cell_size_x, min_cell_size_y };

    // minimum cell size
    return { median_width, median_height };
}

// 3. REFINE PARTITIONS

// distribute object among 4 cells
void distribute_object(
        const obj_t& obj,
        std::vector<obj_t>& top_left,
        std::vector<obj_t>& top_right,
        std::vector<obj_t>& bottom_left,
        std::vector<obj_t>& bottom_right,
        float mid_x,
        float mid_y
) {
    if (obj.low0 <= mid_x && obj.low1 <= mid_y) {
        bottom_left.push_back(obj);
    }

    if (obj.high0 >= mid_x && obj.low1 <= mid_y) {
        bottom_right.push_back(obj);
    }

    if (obj.low0 <= mid_x && obj.high1 >= mid_y) {
        top_left.push_back(obj);
    }

    if (obj.high0 >= mid_x && obj.high1 >= mid_y) {
        top_right.push_back(obj);
    }
}

// 4. PREPARE PARTITIONS

void pack_objects_to_pages(
        const std::vector<obj_t>& objects,
        std::vector<page_t, aligned_allocator<page_t>>& pages
) {
    int num_objects = objects.size();
    int num_pages = (num_objects + MAX_OBJS_PER_PAGE - 1) / MAX_OBJS_PER_PAGE;
    int obj_idx = 0;

    for (int page_idx = 0; page_idx < num_pages; page_idx++) {
        page_t page;

        std::memset(page.data(), 0, 64);

        for (int object_count = 0; object_count < MAX_OBJS_PER_PAGE && obj_idx < num_objects; object_count++, obj_idx++) {
            int start_idx = object_count * 20;
            const obj_t& obj = objects[obj_idx];

            // copy
            std::memcpy(&page[start_idx], &obj.id, sizeof(int));
            std::memcpy(&page[start_idx + 4], &obj.low0, sizeof(float));
            std::memcpy(&page[start_idx + 8], &obj.high0, sizeof(float));
            std::memcpy(&page[start_idx + 12], &obj.low1, sizeof(float));
            std::memcpy(&page[start_idx + 16], &obj.high1, sizeof(float));
        }

        pages.push_back(page);
    }
}

void pack_partitions_to_pages(
        const std::vector<obj_t>& objects,
        const std::vector<partition_meta_t>& meta_A,
        std::vector<page_t, aligned_allocator<page_t>>& pages
) {
    int offset = 0;

    for (const auto& meta : meta_A) {
        std::vector<obj_t> partition_objects(objects.begin() + offset, objects.begin() + offset + meta.count);
        pack_objects_to_pages(partition_objects, pages);
        offset += meta.count;
    }
}

void pack_meta_to_pages(
        const std::vector<partition_meta_t>& meta,
        std::vector<page_t, aligned_allocator<page_t>>& pages
) {
    page_t page = {};
    int meta_count = 0;

    for (const auto& m : meta) {
        int start_idx = meta_count * 12;

        // Pack the metadata into the page
        std::memcpy(&page[start_idx], &m.count, sizeof(int));
        std::memcpy(&page[start_idx + 4], &m.x, sizeof(float));
        std::memcpy(&page[start_idx + 8], &m.y, sizeof(float));

        meta_count++;

        if (meta_count == MAX_META_PER_PAGE) {
            pages.push_back(page);
            page.fill(0);
            meta_count = 0;
        }
    }

    // partially filled page
    if (meta_count > 0) {
        pages.push_back(page);
    }
}

// unpack objects from a page
void unpack_page(
        const page_t& page,
        std::vector<obj_t>& objects,
        int num_objects_in_page
) {
    for (int i = 0; i < num_objects_in_page; i++) {
        obj_t obj;

        std::memcpy(&obj.id, &page[i * 20], sizeof(int));
        std::memcpy(&obj.low0, &page[i * 20 + 4], sizeof(float));
        std::memcpy(&obj.high0, &page[i * 20 + 8], sizeof(float));
        std::memcpy(&obj.low1, &page[i * 20 + 12], sizeof(float));
        std::memcpy(&obj.high1, &page[i * 20 + 16], sizeof(float));

        objects.push_back(obj);
    }
}

void unpack_meta_from_pages(
        std::vector<page_t, aligned_allocator<page_t>>& pages,
        std::vector<partition_meta_t>& meta,
        int num_partitions
) {
    int meta_count = 0;

    for (const auto& page : pages) {
        for (int i = 0; i < MAX_META_PER_PAGE; i++) {
            if (meta_count >= num_partitions) {
                return;
            }

            partition_meta_t m;
            int start_idx = i * 12;

            // copy to partition_meta_t
            std::memcpy(&m.count, &page[start_idx], sizeof(int));
            std::memcpy(&m.x, &page[start_idx + 4], sizeof(float));
            std::memcpy(&m.y, &page[start_idx + 8], sizeof(float));

            meta.push_back(m);
            meta_count++;
        }
    }
}

void print_results(
        host_partition_result_t& partition_result
        ) {
    printf("TIME[cpu_init_time]: %.2lf ms.\n", partition_result.init_grid_time);
    printf("TIME[cpu_partition_time]: %.2lf ms.\n", partition_result.partition_objects_time);
    printf("TIME[cpu_refine_time]: %.2lf ms.\n", partition_result.refine_partitions_time);
    printf("TIME[cpu_prepare_time]: %.2lf ms.\n", partition_result.prepare_partitions_time);
    printf("TIME[cpu_time]: %.2lf ms.\n", partition_result.init_grid_time + partition_result.partition_objects_time + partition_result.refine_partitions_time + partition_result.prepare_partitions_time);
}