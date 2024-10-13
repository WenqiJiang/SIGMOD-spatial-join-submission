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
#include <unordered_set>

#include "types.hpp"
#include "partition_utils.hpp"
#include "utils.hpp"
#include "join.hpp"

// 2. PARTITION OBJECTS

// get indices of object's partitions
std::pair<int, int> calculate_grid_indices(float min_coord, float max_coord, float global_min, float cell_size, int num_cells) {
    int min_index = std::max(0, static_cast<int>((min_coord - global_min) / cell_size));
    int max_index = std::min(num_cells - 1, static_cast<int>((max_coord - global_min) / cell_size));

    return std::make_pair(min_index, max_index);
}

// move obj to partitions
void partition_objects(
        const std::vector<obj_t> &dataset_a,
        const std::vector<obj_t> &dataset_b,
        std::vector<cell_t> &grid,
        int num_partitions,
        float map_min_x, float map_min_y,
        float cell_width, float cell_height
) {
    // dataset A
    for (size_t i = 0; i < dataset_a.size(); i++) {
        const auto& obj = dataset_a[i];
        auto x_indices = calculate_grid_indices(obj.low0, obj.high0, map_min_x, cell_width, num_partitions);
        auto y_indices = calculate_grid_indices(obj.low1, obj.high1, map_min_y, cell_height, num_partitions);

        for (int x = x_indices.first; x <= x_indices.second; x++) {
            for (int y = y_indices.first; y <= y_indices.second; y++) {
                int index = y * num_partitions + x;
                grid[index].objects_a.push_back(obj);
            }
        }
    }

    // dataset B
    for (size_t i = 0; i < dataset_b.size(); i++) {
        const auto& obj = dataset_b[i];
        auto x_indices = calculate_grid_indices(obj.low0, obj.high0, map_min_x, cell_width, num_partitions);
        auto y_indices = calculate_grid_indices(obj.low1, obj.high1, map_min_y, cell_height, num_partitions);

        for (int x = x_indices.first; x <= x_indices.second; x++) {
            for (int y = y_indices.first; y <= y_indices.second; y++) {
                int index = y * num_partitions + x;
                grid[index].objects_b.push_back(obj);
            }
        }
    }
}


void refine_partition(
    cell_t& cell,
    std::vector<cell_t>& final_partitions,
    int max_comparisons_per_partition,
    map_stats_t map_stats
) {
    float cell_width = cell.max_X - cell.min_X;
    float cell_height = cell.max_Y - cell.min_Y;

    bool is_cell_small = cell_width <= map_stats.min_cell_size_x || cell_height <= map_stats.min_cell_size_y;
    bool is_under_comparison_limit = (cell.objects_a.size() * cell.objects_b.size() <= max_comparisons_per_partition &&
                                      cell.objects_b.size() <= MAX_OBJS_PER_PARTITION);

    // all ok -> add to final_partitions
    if (is_cell_small || is_under_comparison_limit) {
        final_partitions.push_back(cell);
        return;
    }

    // midpoints
    float mid_x = (cell.min_X + cell.max_X) / 2;
    float mid_y = (cell.min_Y + cell.max_Y) / 2;

    // new smaller partitions
    cell_t top_left = { {}, {}, cell.min_X, mid_x, mid_y, cell.max_Y };
    cell_t top_right = { {}, {}, mid_x, cell.max_X, mid_y, cell.max_Y };
    cell_t bottom_left = { {}, {}, cell.min_X, mid_x, cell.min_Y, mid_y };
    cell_t bottom_right = { {}, {}, mid_x, cell.max_X, cell.min_Y, mid_y };

    // move objs from A
    for (const auto& obj : cell.objects_a) {
        distribute_object(obj, top_left.objects_a, top_right.objects_a, bottom_left.objects_a, bottom_right.objects_a, mid_x, mid_y);
    }

    // move objs from B
    for (const auto& obj : cell.objects_b) {
        distribute_object(obj, top_left.objects_b, top_right.objects_b, bottom_left.objects_b, bottom_right.objects_b, mid_x, mid_y);
    }

    // do the same with the new partitions
    refine_partition(top_left, final_partitions, max_comparisons_per_partition, map_stats);
    refine_partition(top_right, final_partitions, max_comparisons_per_partition, map_stats);
    refine_partition(bottom_left, final_partitions, max_comparisons_per_partition, map_stats);
    refine_partition(bottom_right, final_partitions, max_comparisons_per_partition, map_stats);
}

void refine_partitions(
    std::vector<cell_t>& initial_grid,
    std::vector<cell_t>& final_partitions,
    int max_comparisons_per_partition,
    map_stats_t map_stats
) {
    for (int i = 0; i < initial_grid.size(); i++) {
        refine_partition(initial_grid[i], final_partitions, max_comparisons_per_partition, map_stats);
    }
}

// 4. PREPARE PARTITIONS

prepare_partitions_result_t prepare_partitions(
        std::vector<cell_t>& final_partitions,
        std::vector<page_t, aligned_allocator<page_t>>& partitions_A,
        std::vector<page_t, aligned_allocator<page_t>>& meta_pages_A,
        std::vector<page_t, aligned_allocator<page_t>>& partitions_B,
        std::vector<page_t, aligned_allocator<page_t>>& meta_pages_B
) {

    int count = 0;
    std::vector<partition_meta_t> meta_A;
    std::vector<partition_meta_t> meta_B;

    int total_objects_A = 0;
    int total_objects_B = 0;
    size_t max_objects_A = 0;
    size_t max_objects_B = 0;

    std::vector<obj_t> all_objects_A;
    std::vector<obj_t> all_objects_B;

    // step 1: fill temporary buffers
    for (int i = 0; i < final_partitions.size(); i++) {
        const auto& cell = final_partitions[i];

        // skip not possible ones
        if (cell.objects_a.empty() || cell.objects_b.empty()) {
            continue;
        }

        size_t objects_A_count = cell.objects_a.size();
        size_t objects_B_count = cell.objects_b.size();

        total_objects_A += objects_A_count;
        total_objects_B += objects_B_count;
        max_objects_A = std::max(max_objects_A, objects_A_count);
        max_objects_B = std::max(max_objects_B, objects_B_count);

        // append objs to temporary buffers
        all_objects_A.insert(all_objects_A.end(), cell.objects_a.begin(), cell.objects_a.end());
        all_objects_B.insert(all_objects_B.end(), cell.objects_b.begin(), cell.objects_b.end());

        // create metadata
        partition_meta_t cell_meta_A = {objects_A_count, cell.min_X, cell.min_Y};
        partition_meta_t cell_meta_B = {objects_B_count, cell.max_X, cell.max_Y};

        meta_A.push_back(cell_meta_A);
        meta_B.push_back(cell_meta_B);

        count++;
    }

    // step 2: pack temporary buffers into pages
    pack_partitions_to_pages(all_objects_A, meta_A, partitions_A);
    pack_partitions_to_pages(all_objects_B, meta_B, partitions_B);

    pack_meta_to_pages(meta_A, meta_pages_A);
    pack_meta_to_pages(meta_B, meta_pages_B);

    partition_stats_t stats_A = {
            total_objects_A,
            count > 0 ? static_cast<double>(total_objects_A) / count : 0,
            max_objects_A
    };

    partition_stats_t stats_B = {
            total_objects_B,
            count > 0 ? static_cast<double>(total_objects_B) / count : 0,
            max_objects_B
    };

    return {count, stats_A, stats_B};
}


//host_partition_result_t host_partition(
host_partition_result_t host_partition(
        std::vector<obj_t> &dataset_a,
        std::vector<obj_t> &dataset_b,
        int num_initial_partitions_1d,
        int max_comparisons_per_partition,
        bool validate_intermediate = false
) {
    if (validate_intermediate) {
        join_datasets(
                dataset_a,
                dataset_b
        );
    }

    // STEP 1. Prepare the grid.

    auto start = std::chrono::high_resolution_clock::now();

    auto map_bounds = get_map_bounds(dataset_a, dataset_b);

    float cell_width = (map_bounds.max_x - map_bounds.min_x) / num_initial_partitions_1d;
    float cell_height = (map_bounds.max_y - map_bounds.min_y) / num_initial_partitions_1d;

    auto grid = init_grid_1D(
            num_initial_partitions_1d,
            map_bounds,
            cell_width, cell_height
    );

    // min cell size
    auto map_stats = compute_map_stats(dataset_a, dataset_b);

    auto init_grid_end = std::chrono::high_resolution_clock::now();

    // STEP 2. Partition objects.

    partition_objects(
            dataset_a,
            dataset_b,
            grid,
            num_initial_partitions_1d,
            map_bounds.min_x, map_bounds.min_y,
            cell_width, cell_height
    );

    auto partition_objects_end = std::chrono::high_resolution_clock::now();

    if (validate_intermediate) {
        join_grid(
            grid
        );
    }

    // STEP 3. Adjust the partitioned grid.

    std::vector<cell_t> refined_partitions;

    refine_partitions(
        grid,
        refined_partitions,
        max_comparisons_per_partition,
        map_stats
    );

    auto refine_partitions_end = std::chrono::high_resolution_clock::now();

    if (validate_intermediate) {
        join_grid(
                refined_partitions
        );
    }

    // STEP 4. Prepare partitions for processing on the FPGA.

    std::vector<page_t, aligned_allocator<page_t>> partitions_A;
    std::vector<page_t, aligned_allocator<page_t>> meta_A;
    std::vector<page_t, aligned_allocator<page_t>> partitions_B;
    std::vector<page_t, aligned_allocator<page_t>> meta_B;

    auto partition_stats = prepare_partitions(
            refined_partitions,
            partitions_A, meta_A,
            partitions_B, meta_B
    );

    auto end = std::chrono::high_resolution_clock::now();

    if (validate_intermediate) {
        auto results = join_partitions(
                partitions_A,
                meta_A,
                partitions_B,
                meta_B,
                partition_stats.num_partitions
        );
    }

    std::cout << std::endl;
    std::cout << "Num initial partitions: " << grid.size() << std::endl;
    std::cout << "Num refined partitions: " << refined_partitions.size() << std::endl;
    std::cout << "Num final partitions: " << partition_stats.num_partitions << std::endl;

    std::cout << "Average count of A: " << partition_stats.stats_A.average_objects << std::endl;
    std::cout << "Average count of B: " << partition_stats.stats_B.average_objects << std::endl;

    std::cout << "Total pages of A: " << partitions_A.size() << std::endl;
    std::cout << "Total pages of B: " << partitions_B.size() << std::endl;

    std::cout << "Total count of A: " << partition_stats.stats_A.total_objects << std::endl;
    std::cout << "Total count of B: " << partition_stats.stats_B.total_objects << std::endl;

    std::cout << "Max count of A: " << partition_stats.stats_A.max_objects << std::endl;
    std::cout << "Max count of B: " << partition_stats.stats_B.max_objects << std::endl;

    return {
        partitions_A,
        meta_A,
        partitions_B,
        meta_B,

        partition_stats,

        (std::chrono::duration_cast<std::chrono::microseconds>(init_grid_end - start).count()) / 1000.0,
        (std::chrono::duration_cast<std::chrono::microseconds>(partition_objects_end - init_grid_end).count()) / 1000.0,
        (std::chrono::duration_cast<std::chrono::microseconds>(refine_partitions_end - partition_objects_end).count()) / 1000.0,
        (std::chrono::duration_cast<std::chrono::microseconds>(end - refine_partitions_end).count()) / 1000.0
    };
}

