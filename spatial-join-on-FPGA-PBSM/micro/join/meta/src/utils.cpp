#pragma once

#include <iostream>
#include <fstream>
#include <vector>

#include "types.hpp"


// from: Wenqi
template <typename T>
struct aligned_allocator
{
    using value_type = T;
    T* allocate(std::size_t num)
    {
        void* ptr = nullptr;
        if (posix_memalign(&ptr,4096,num*sizeof(T)))
            throw std::bad_alloc();
        return reinterpret_cast<T*>(ptr);
    }
    void deallocate(T* p, std::size_t num)
    {
        free(p);
    }
};


// read the binfile
std::vector<int, aligned_allocator<int> > read_from_binary_file(const std::string& file_path, int& num_objects_A) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
        std::cerr << "Could not open file: " << file_path << std::endl;
        exit(EXIT_FAILURE);
    }

    // read size
    file.read(reinterpret_cast<char*>(&num_objects_A), 64);

    // calculate vector size (pay attention to padding)
    size_t total_size_bytes = std::ceil(static_cast<double>(num_objects_A) / MAX_OBJS_PER_PAGE) * PAGE_SIZE_BYTES;
    size_t total_size_ints = total_size_bytes / sizeof(int);

    // vector-buffer
    std::vector<int, aligned_allocator<int> > buffer(total_size_ints);

    // skip reading number of objects (first 64B)
    file.seekg(PAGE_SIZE_BYTES, std::ios::beg);

    // read into buffer
    file.read(reinterpret_cast<char*>(buffer.data()), total_size_bytes);

    return buffer;
}

// read objects from file
std::vector<obj_t, aligned_allocator<obj_t>> read_objects_from_file(const std::string& file_path) {
    std::ifstream file(file_path);
    if (!file) {
        std::cerr << "Could not open file: " << file_path << std::endl;
        exit(EXIT_FAILURE);
    }

    // read number of objects (but ignore for now)
    int num_objects;
    file >> num_objects;

    std::vector<obj_t, aligned_allocator<obj_t>> objects;

    for (int i = 0; i < num_objects; ++i) {
        obj_t obj;
        // id + coordinates
        if (file >> obj.id >> obj.low0 >> obj.high0 >> obj.low1 >> obj.high1) {
            objects.push_back(obj);
        } else {
            std::cerr << "ERROR: Could not read object data from file!" << std::endl;
            break;
        }
    }

    return objects;
}