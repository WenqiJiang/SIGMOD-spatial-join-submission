import argparse
import struct
import math

PAGE_SIZE_BYTES = 64
# 20 bytes (1 * id + 4 * boundary)
OBJ_SIZE_BYTES = 20
MAX_OBJS_PER_PAGE = 3


def read_data_file(filepath):
    with open(filepath, 'rb') as file:
        # first 64 bytes -> the number of objects
        header = file.read(64)
        num_objects = struct.unpack('i', header[:4])[0]

        # total size in bytes
        total_size_bytes = math.ceil(num_objects / MAX_OBJS_PER_PAGE) * PAGE_SIZE_BYTES

        # skip the first 64 bytes
        file.seek(PAGE_SIZE_BYTES, 0)

        # read the rest
        buffer = file.read(total_size_bytes)

    return buffer, num_objects


class ObjT:
    def __init__(self, id, low0, high0, low1, high1):
        self.id = id
        self.low0 = low0
        self.high0 = high0
        self.low1 = low1
        self.high1 = high1


def parse_data_pages(buffer, num_objects):
    objs = []
    num_pages = math.ceil(num_objects / MAX_OBJS_PER_PAGE)

    for i in range(num_pages):
        # page is 512 bits (64 bytes)
        page_start = i * PAGE_SIZE_BYTES
        page = buffer[page_start:page_start + PAGE_SIZE_BYTES]

        # number of obj in this page
        objs_in_current_page = MAX_OBJS_PER_PAGE
        if i == num_pages - 1:
            objs_in_current_page = num_objects - len(objs)

        # parse
        for j in range(objs_in_current_page):
            id_A = struct.unpack('i', page[j * OBJ_SIZE_BYTES:j * OBJ_SIZE_BYTES + 4])[0]
            low0_A = struct.unpack('f', page[j * OBJ_SIZE_BYTES + 4:j * OBJ_SIZE_BYTES + 8])[0]
            high0_A = struct.unpack('f', page[j * OBJ_SIZE_BYTES + 8:j * OBJ_SIZE_BYTES + 12])[0]
            low1_A = struct.unpack('f', page[j * OBJ_SIZE_BYTES + 12:j * OBJ_SIZE_BYTES + 16])[0]
            high1_A = struct.unpack('f', page[j * OBJ_SIZE_BYTES + 16:j * OBJ_SIZE_BYTES + 20])[0]

            obj = ObjT(id_A, low0_A, high0_A, low1_A, high1_A)
            objs.append(obj)

    return objs


def read_rtree_file(filepath, page_bytes):
    nodes = []
    node_count = 0
    total_leaf_objects = 0

    with open(filepath, 'rb') as file:
        while True:
            node_data = file.read(page_bytes)
            if not node_data:
                break

            # node meta (first 64 bytes)
            is_leaf, count, obj_id, low0, high0, low1, high1 = struct.unpack('iii4f', node_data[:28])
            node_meta = ObjT(obj_id, low0, high0, low1, high1)

            node = {
                "is_leaf": is_leaf,
                "count": count,
                "meta": node_meta,
                "children": []
            }

            # node meta information
            print(f"Node {node_count}:")
            print(f"  is_leaf: {is_leaf}, count: {count}, id: {obj_id}")
            print(f"  Bounding Rectangle: [{low0}, {high0}], [{low1}, {high1}]")

            # a leaf node -> read the objects
            if is_leaf:
                total_leaf_objects += count
                for i in range(count):
                    # !!!consider padding!!!
                    start = 64 + (i // 3) * 64 + (i % 3) * OBJ_SIZE_BYTES
                    child_data = struct.unpack('i4f', node_data[start:start + OBJ_SIZE_BYTES])
                    child_obj = ObjT(*child_data)
                    node["children"].append(child_obj)

                    print(f"  Leaf Object {i}: id: {child_obj.id}")
                    print(f"    Bounding Rectangle: [{child_obj.low0}, {child_obj.high0}], [{child_obj.low1}, {child_obj.high1}]")
            else:
                # a non-leaf node -> read child node references
                for i in range(count):
                    # consider padding!!!
                    start = 64 + (i // 3) * 64 + (i % 3) * OBJ_SIZE_BYTES
                    child_data = struct.unpack('i4f', node_data[start:start + OBJ_SIZE_BYTES])
                    child_node_ref = ObjT(*child_data)
                    node["children"].append(child_node_ref)

                    print(f"  Non-Leaf Child {i}: Node id: {child_node_ref.id}")
                    print(f"    Bounding Rectangle: [{child_node_ref.low0}, {child_node_ref.high0}], [{child_node_ref.low1}, {child_node_ref.high1}]")

            print(f"Node {node_count} processed")
            print("------------------------------------------------")

            nodes.append(node)
            node_count += 1

    print(f"Total nodes read: {node_count}")
    print(f"Total objects in leaf nodes: {total_leaf_objects}")
    return nodes


def intersects(obj1, obj2):
    return (obj1.low0 <= obj2.high0 and obj1.high0 >= obj2.low0) and \
        (obj1.low1 <= obj2.high1 and obj1.high1 >= obj2.low1)


def join_with_rtree(object_list, rtree_nodes):
    matches = 0
    # avoid duplicates
    # matched_object_ids = set()
    total_objects = len(object_list)
    object_counter = 0

    for obj in object_list:
        object_counter += 1
        print(f"Processing object {object_counter}/{total_objects} (ID: {obj.id})...")

        stack = []
        for node in rtree_nodes:
            if node['meta'].id == 0:
                stack.append(node)

        node_counter = 0
        while stack:
            node = stack.pop()
            node_counter += 1

            if intersects(obj, node['meta']):
                if node['is_leaf']:
                    print(f"  Checking leaf node {node_counter} with {node['count']} objects...")
                    for child_obj in node['children']:
                        if intersects(obj, child_obj): # and child_obj.id not in matched_object_ids:
                            matches += 1
                            # matched_object_ids.add(child_obj.id)
                            print(f"  Match found: Object {obj.id} intersects with leaf object {child_obj.id}")
                else:
                    print(f"  Checking non-leaf node {node_counter} with {node['count']} children...")
                    for child_node_ref in node['children']:
                        for child_node in rtree_nodes:
                            if child_node['meta'].id == child_node_ref.id:
                                stack.append(child_node)

    print(f"Total objects processed: {object_counter}")
    print(f"Total matches found: {matches}")

    return matches


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Join objects with a spatial R-tree index.")
    parser.add_argument('--filepathA', type=str, required=True, help="Path to the first binary file")
    parser.add_argument('--filepathB', type=str, required=True, help="Path to the R-tree file")

    args = parser.parse_args()

    # read A
    bufferA, num_objects_A = read_data_file(args.filepathA)
    objectsA = parse_data_pages(bufferA, num_objects_A)

    tree_max_node_entries_count = 16

    # page_bytes for the tree
    page_bytes = (1 + (tree_max_node_entries_count + MAX_OBJS_PER_PAGE - 1) // MAX_OBJS_PER_PAGE) * PAGE_SIZE_BYTES

    # read tree B
    rtree_nodes = read_rtree_file(args.filepathB, page_bytes)