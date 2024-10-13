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

// read objects from file
std::vector<obj_t> read_objects_from_file(const std::string &file_path) {
    std::ifstream file(file_path);
    if (!file) {
        std::cerr << "Could not open file: " << file_path << std::endl;
        exit(EXIT_FAILURE);
    }

    // read number of objects (but ignore for now)
    int num_objects;
    file >> num_objects;

    std::vector<obj_t> objects;

    for (int i = 0; i < num_objects; i++) {
        obj_t obj;
        // id + coordinates
        if (file >> obj.id >> obj.low0 >> obj.high0 >> obj.low1 >> obj.high1) {
            // filter out invalid rows (0 0 0 0 0)
            if (obj.id != 0 || obj.low0 != 0 || obj.high0 != 0 || obj.low1 != 0 || obj.high1 != 0) {
                objects.push_back(obj);
            }
        } else {
            std::cerr << "ERROR: Could not read object data from file!" << std::endl;
            break;
        }
    }

    return objects;
}
