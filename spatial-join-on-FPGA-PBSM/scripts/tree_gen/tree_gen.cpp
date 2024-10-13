#include <iostream>
#include <iostream>
#include <vector>
#include <queue>
#include <unordered_set>
#include <fstream>
#include <limits>
#include <cmath>
#include <algorithm>
#include <stdexcept>

struct mbr_t {
    float low0;
    float high0;
    float low1;
    float high1;
};

struct obj_t {
    int id;  // obj id for leaf nodes; child node id for directory nodes
    mbr_t mbr;  // minimum bounding rectangle
};

struct node_meta_t {
    int is_leaf;  // bool: 1 if the node is a leaf, 0 otherwise
    int count;    // number of valid items in the node
    obj_t obj;    // MBR and id
};

class MBR {
private:
    float _low0, _high0;
    float _low1, _high1;

public:
    MBR() {};

    MBR(float low0, float high0, float low1, float high1) : _low0(low0), _high0(high0),
                                                            _low1(low1), _high1(high1) {}

    inline float get_low0() const {
        return _low0;
    }

    inline float get_high0() const {
        return _high0;
    }

    inline float get_low1() const {
        return _low1;
    }

    inline float get_high1() const {
        return _high1;
    }

    inline bool intersects(const MBR *const other) const {
        return ((_low0 <= other->get_high0()) && (_high0 >= other->get_low0()) &&
                (_low1 <= other->get_high1()) && (_high1 >= other->get_low1()));
    }

    friend std::ostream &operator<<(std::ostream &out, const MBR &r) {
        return out << "low 0: " << r.get_low0()
                   << "\thigh 0: " << r.get_high0()
                   << "\tlow1 : " << r.get_low1()
                   << "\thigh 1: " << r.get_high1();
    }
};

class Node {
public:
    int node_id;
    bool leaf;
    int count;
    MBR mbr;
    std::vector<Node*> child_ptrs;
    std::vector<int> obj_ids;
    std::vector<MBR*> mbrs;

    Node(int node_id, bool is_leaf, int size) : node_id(node_id), leaf(is_leaf), count(0) {
        child_ptrs.reserve(size);
        obj_ids.reserve(size);
        mbrs.reserve(size);
    }

    int get_node_id() const { return node_id; }
    bool is_leaf() const { return leaf; }
    int get_count() const { return count; }

    void add_entry(MBR *mbr) {
        mbrs.push_back(mbr);
        count++;
    }

    void add_entry(MBR *mbr, int obj_id) {
        if (!obj_ids.empty() && std::find(obj_ids.begin(), obj_ids.end(), obj_id) != obj_ids.end()) return; // Avoid duplicates
        mbrs.push_back(mbr);
        obj_ids.push_back(obj_id);
        count++;
    }

    void add_entry(Node *child_ptr) {
        mbrs.push_back(&child_ptr->mbr);
        child_ptrs.push_back(child_ptr);
        count++;
    }

    Node* get_child(int i) const { return child_ptrs.at(i); }
    int get_obj_id(int i) const { return obj_ids.at(i); }
    MBR* get_mbr(int i) const { return mbrs.at(i); }

    void merge_mbrs(MBR *bounding) {
        float min0 = bounding->get_high0(), max0 = bounding->get_low0();
        float min1 = bounding->get_high1(), max1 = bounding->get_low1();

        for (const MBR *m : mbrs) {
            min0 = std::min(min0, m->get_low0());
            max0 = std::max(max0, m->get_high0());
            min1 = std::min(min1, m->get_low1());
            max1 = std::max(max1, m->get_high1());
        }

        mbr = MBR(min0, max0, min1, max1);
    }
};

Node *root_A;
std::vector<MBR*> agents;

int max_entries = 16;
double fill_factor = 1.0;

void collect_all_nodes(std::vector<Node*>& node_list, Node *root) {
    std::queue<Node*> candidate_node_list;
    candidate_node_list.push(root);

    while (!candidate_node_list.empty()) {
        Node *current_node = candidate_node_list.front();
        candidate_node_list.pop();
        node_list.push_back(current_node);

        if (!current_node->is_leaf()) {
            for (int i = 0; i < current_node->get_count(); i++) {
                candidate_node_list.push(current_node->get_child(i));
            }
        }
    }

    std::sort(node_list.begin(), node_list.end(), [](Node *node_A, Node *node_B) {
        return node_A->get_node_id() < node_B->get_node_id();
    });
}

void node_serialisation(char *buffer, uint32_t page_bytes, Node *node) {
    uint32_t offset = 0;
    const uint32_t meta_size = 64;  // size of the node_meta_t structure
    const uint32_t obj_block_size = 64;  // size of each object block
    const uint32_t max_objs_per_block = 3;  // max objects per 64-byte block
    const uint32_t usable_bytes_per_block = obj_block_size - 4;  // 60 bytes usable per block

    // Step 1: serialise the node's metadata
    node_meta_t meta;
    meta.is_leaf = node->is_leaf();
    meta.count = node->get_count();
    meta.obj.id = node->get_node_id();
    meta.obj.mbr.low0 = node->mbr.get_low0();
    meta.obj.mbr.high0 = node->mbr.get_high0();
    meta.obj.mbr.low1 = node->mbr.get_low1();
    meta.obj.mbr.high1 = node->mbr.get_high1();

    memcpy(buffer + offset, &meta, sizeof(node_meta_t));
    offset += meta_size;

    // padding after the meta
    memset(buffer + offset, 0, obj_block_size - meta_size);
    offset += (obj_block_size - meta_size);

    // Step 2: serialise the objects/children in 64-byte blocks
    int count = node->get_count();

    for (int i = 0; i < count; i++) {
        if (i > 0 && i % max_objs_per_block == 0) {
            // align t to the next 64-byte block
            uint32_t block_padding_size = obj_block_size - (offset % obj_block_size);

            if (block_padding_size > 0 && block_padding_size < obj_block_size) {
                memset(buffer + offset, 0, block_padding_size);
                offset += block_padding_size;
            }
        }

        // a leaf node -> serialise object IDs and their MBRs
        // otherwise -> serialise child node IDs and their MBRs
        obj_t obj;

        if (node->is_leaf()) {
            obj.id = node->get_obj_id(i);
            obj.mbr.low0 = node->get_mbr(i)->get_low0();
            obj.mbr.high0 = node->get_mbr(i)->get_high0();
            obj.mbr.low1 = node->get_mbr(i)->get_low1();
            obj.mbr.high1 = node->get_mbr(i)->get_high1();
        } else {
            obj.id = node->get_child(i)->get_node_id();
            obj.mbr.low0 = node->get_child(i)->mbr.get_low0();
            obj.mbr.high0 = node->get_child(i)->mbr.get_high0();
            obj.mbr.low1 = node->get_child(i)->mbr.get_low1();
            obj.mbr.high1 = node->get_child(i)->mbr.get_high1();
        }

        // copy the object data into the buffer
        memcpy(buffer + offset, &obj, sizeof(obj_t));
        offset += sizeof(obj_t);
    }

    // Step 3: final padding to fill up the page size
    if (offset < page_bytes) {
        memset(buffer + offset, 0, page_bytes - offset);
    }
}


void index_serialisation(Node *root, const char *file_name, int max_entries) {
    std::vector<Node*> node_list;
    collect_all_nodes(node_list, root);

    std::ofstream file(file_name, std::ios::out | std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open output file.");
    }

    int header_bytes = 64;
    int data_bytes = max_entries % 3 == 0 ? (max_entries / 3) * 64 : (max_entries / 3 + 1) * 64;
    int node_bytes = header_bytes + data_bytes;

    for (Node* node : node_list) {
        std::vector<char> buffer(node_bytes);
        node_serialisation(buffer.data(), node_bytes, node);
        file.write(buffer.data(), node_bytes);
    }

    file.close();
}

bool cmp_x(MBR *mbr_A, MBR *mbr_B) {
    return mbr_A->get_low0() < mbr_B->get_low0();
}

bool cmp_y(MBR *mbr_A, MBR *mbr_B) {
    return mbr_A->get_low1() < mbr_B->get_low1();
}

void fix_tree(Node *root)
{
    int id_counter = 0;

    // Queue to keep track of unexplored nodes
    std::queue<Node *> nodes;
    nodes.push(root);
    while (!nodes.empty())
    {
        Node *curr = nodes.front();
        // fix node_id
        curr->node_id = id_counter;
        id_counter++;
        // // fix mbr
        // curr->merge_mbrs(10000);
        nodes.pop();

        if (!curr->leaf)
        {
            for (int i = 0; i < curr->count; i++)
            {
                nodes.push(curr->get_child(i));
            }
        }
    }
}

void build(std::vector<Node*>& internalNodes, MBR *bounding) {
    if (internalNodes.size() == 1) {
        root_A = internalNodes.at(0);
    } else {
        std::vector<Node*> tempNodes;
        uint32_t numInNode = static_cast<uint32_t>(fill_factor * max_entries);

        for (int i = 0; i < static_cast<int>(internalNodes.size()) - static_cast<int>(numInNode); i += numInNode) {
            Node *inode = new Node(-1, false, max_entries);

            for (int k = i; k < i + numInNode; k++) {
                inode->add_entry(internalNodes.at(k));
            }

            inode->merge_mbrs(bounding);
            tempNodes.push_back(inode);
        }

        int rem = internalNodes.size() % numInNode;
        if (rem == 0) rem = numInNode;
        Node *inode = new Node(-1, false, max_entries);

        for (int k = internalNodes.size() - rem; k < internalNodes.size(); k++) {
            inode->add_entry(internalNodes.at(k));
        }

        inode->merge_mbrs(bounding);
        tempNodes.push_back(inode);

        build(tempNodes, bounding);
    }
}

void bulk_load(std::vector<MBR*> *agents, MBR *bounding) {
    uint32_t num_in_leaf = static_cast<uint32_t>(fill_factor * max_entries);
    uint32_t P = std::ceil(static_cast<double>(agents->size()) / num_in_leaf);
    uint32_t S = std::sqrt(P);

    // sort agents by x-coordinates
    std::sort(agents->begin(), agents->end(), cmp_x);

    // number of points in a slice
    uint32_t slice_num = S * num_in_leaf;
    int i;
    for (i = 0; i < static_cast<int>(agents->size()) - static_cast<int>(slice_num); i += slice_num) {
        // sort within each slice by y-coordinates
        std::sort(agents->begin() + i, agents->begin() + i + slice_num, cmp_y);
    }

    // sort remaining elements by y-coordinate
    std::sort(agents->begin() + i, agents->end(), cmp_y);

    // build leaf nodes
    std::vector<Node*> temp_nodes;
    for (int i = 0; i < static_cast<int>(agents->size()) - static_cast<int>(num_in_leaf); i += num_in_leaf) {
        Node *leaf = new Node(-1, true, max_entries);

        for (int k = i; k < i + num_in_leaf; k++) {
            leaf->add_entry(agents->at(k), k);
        }

        leaf->merge_mbrs(bounding);
        temp_nodes.push_back(leaf);
    }

    // handle remaining objects

    // check if there is actually a remainder
    if (agents->size() % num_in_leaf != 0) {
        Node *leaf = new Node(-1, true, max_entries);

        // k = the start of the last incomplete segment, !! if it exists !!, so only the remainder objects are processed,
        // preventing duplicate objects
        for (int k = static_cast<int>(agents->size()) - (agents->size() % num_in_leaf); k < agents->size(); k++) {
            leaf->add_entry(agents->at(k), k);
        }

        leaf->merge_mbrs(bounding);
        temp_nodes.push_back(leaf);
    }

    // build the tree from leaf nodes
    build(temp_nodes, bounding);
}

void find_and_print_duplicates(Node* root) {
    if (!root) {
        std::cerr << "Tree is empty." << std::endl;
        return;
    }

    std::unordered_set<int> unique_ids;
    std::vector<int> duplicates;

    // queue for level-order traversal
    std::vector<Node*> node_queue;
    node_queue.push_back(root);

    while (!node_queue.empty()) {
        Node* current_node = node_queue.back();
        node_queue.pop_back();

        if (current_node->is_leaf()) {
            // check for duplicates
            for (int i = 0; i < current_node->get_count(); i++) {
                int obj_id = current_node->get_obj_id(i);

                if (unique_ids.find(obj_id) != unique_ids.end()) {
                    duplicates.push_back(obj_id);
                } else {
                    unique_ids.insert(obj_id);
                }
            }
        } else {
            // not a leaf node -> enqueue all child nodes
            for (int i = 0; i < current_node->get_count(); i++) {
                node_queue.push_back(current_node->get_child(i));
            }
        }
    }

    if (!duplicates.empty()) {
        std::cout << "Found duplicate object IDs:" << std::endl;
        for (int id : duplicates) {
            std::cout << "Duplicate object ID: " << id << std::endl;
        }
    } else {
        std::cout << "No duplicates found in the tree." << std::endl;
    }
}

// get the output file name based on the input file name
std::string get_output_file_name(const std::string& input_file_path) {
    std::filesystem::path input_path(input_file_path);

    std::string file_name = input_path.stem().string();
    std::string base_name = "tree_" + file_name + ".bin";

    std::cout << "Generated output file name: " << base_name << std::endl;

    return base_name;
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: ./tree_gen trace_file\n";
        return 1;
    }

    const char *file_name = argv[1];
    std::cout << "Opening file: " << file_name << std::endl;
    FILE *trace_file = fopen(file_name, "r");
    if (!trace_file) {
        std::cerr << "Failed to open trace file.\n";
        return 1;
    }

    int num_rows;
    fscanf(trace_file, "%d\n", &num_rows);
    std::cout << "Number of rows: " << num_rows << std::endl;

    float lowest0 = std::numeric_limits<float>::max();
    float highest0 = std::numeric_limits<float>::lowest();
    float lowest1 = std::numeric_limits<float>::max();
    float highest1 = std::numeric_limits<float>::lowest();

    for (int i = 0; i < num_rows; i++) {
        int id;
        float low0, high0, low1, high1;
        fscanf(trace_file, "%d %f %f %f %f\n", &id, &low0, &high0, &low1, &high1);

        std::cout << "Row " << i << ": id=" << id << ", low0=" << low0 << ", high0=" << high0
                  << ", low1=" << low1 << ", high1=" << high1 << std::endl;

        lowest0 = std::min(low0, lowest0);
        highest0 = std::max(high0, highest0);
        lowest1 = std::min(low1, lowest1);
        highest1 = std::max(high1, highest1);

        agents.push_back(new MBR(low0, high0, low1, high1));
    }

    std::cout << "Bounds: lowest0=" << lowest0 << ", highest0=" << highest0
              << ", lowest1=" << lowest1 << ", highest1=" << highest1 << std::endl;

    MBR bounding_box(lowest0, highest0, lowest1, highest1);

    std::cout << "Bulk loading agents..." << std::endl;
    bulk_load(&agents, &bounding_box);

    std::cout << "Fixing tree..." << std::endl;
    fix_tree(root_A);

    std::cout << "Serialising index..." << std::endl;
    index_serialisation(root_A, get_output_file_name(file_name).c_str(), max_entries);

    std::cout << "Finding and printing duplicates..." << std::endl;
    find_and_print_duplicates(root_A);

    std::cout << "Closing file: " << file_name << std::endl;
    fclose(trace_file);
    return 0;
}