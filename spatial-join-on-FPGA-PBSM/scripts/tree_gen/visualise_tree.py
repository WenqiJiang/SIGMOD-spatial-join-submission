import argparse
import struct
import math

import pydot


class MBR:
    def __init__(self, low0, high0, low1, high1):
        self.low0 = low0
        self.high0 = high0
        self.low1 = low1
        self.high1 = high1

    def __repr__(self):
        return f"[{self.low0:.2f}, {self.high0:.2f}], [{self.low1:.2f}, {self.high1:.2f}]"


class Node:
    def __init__(self, node_id, is_leaf, mbr, count=0):
        self.node_id = node_id
        self.is_leaf = is_leaf
        self.mbr = mbr
        self.children = []
        self.obj_ids = []
        self.count = count

    def add_child(self, child):
        self.children.append(child)

    def add_object(self, obj_id):
        self.obj_ids.append(obj_id)

    def __repr__(self):
        if self.is_leaf:
            objects_str = "\n".join(f"    Object ID: {obj_id}" for obj_id in self.obj_ids)
            return f"Leaf node(ID: {self.node_id}, MBR: {self.mbr}, Count: {self.count})\nObjects:\n{objects_str}"
        else:
            return f"Internal node(ID: {self.node_id}, MBR: {self.mbr}, Count: {self.count})"


def read_node(buffer, start_offset, meta_size, obj_block_size, max_objs_per_block):
    offset = start_offset

    # meta
    meta_format = 'iiiffff'
    node_meta = struct.unpack_from(meta_format, buffer, offset)
    offset += meta_size

    is_leaf, count, node_id, low0, high0, low1, high1 = node_meta
    mbr = MBR(low0, high0, low1, high1)
    node = Node(node_id, is_leaf, mbr, count)

    # children
    obj_format = 'iffff'
    obj_size = struct.calcsize(obj_format)

    for i in range(count):
        if i > 0 and i % max_objs_per_block == 0:
            # padding
            block_padding_size = obj_block_size - (offset % obj_block_size)
            if block_padding_size > 0 and block_padding_size < obj_block_size:
                offset += block_padding_size

        obj_data = struct.unpack_from(obj_format, buffer, offset)
        offset += obj_size

        obj_id, low0, high0, low1, high1 = obj_data

        if is_leaf:
            node.add_object(obj_id)
        else:
            child_mbr = MBR(low0, high0, low1, high1)
            # temp node
            child_node = Node(obj_id, True, child_mbr)
            node.add_child(child_node)

    return node


def read_rtree(file_name, max_entries):
    with open(file_name, 'rb') as f:
        buffer = f.read()

    nodes = {}
    meta_size = 64
    obj_block_size = 64
    max_objs_per_block = 3

    # total node size = (1 meta + data blocks) * 64 bytes
    total_blocks = 1 + math.ceil(max_entries / max_objs_per_block)
    total_node_size = total_blocks * obj_block_size

    offset = 0
    root = None

    while offset < len(buffer):
        node = read_node(buffer, offset, meta_size, obj_block_size, max_objs_per_block)
        nodes[node.node_id] = node
        offset += total_node_size

        if not root:
            root = node

    # resolve children for non-leaf nodes
    for node in nodes.values():
        if not node.is_leaf:
            node.children = [nodes[child.node_id] for child in node.children]

    return root


def visualise_tree(root):
    graph = pydot.Dot(graph_type='digraph')

    def add_edges(node):
        label = f"{node}"
        graph_node = pydot.Node(str(node.node_id), label=label)
        graph.add_node(graph_node)

        for child in node.children:
            add_edges(child)
            graph.add_edge(pydot.Edge(str(node.node_id), str(child.node_id)))

    add_edges(root)
    graph.write_png('rtree.png')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process and visualise an R-tree.')
    parser.add_argument('file_name', type=str, help='Path to the R-tree file')
    parser.add_argument('max_entries', type=int, help='Max entries for the R-tree node')

    args = parser.parse_args()

    root = read_rtree(args.file_name, args.max_entries)
    visualise_tree(root)
