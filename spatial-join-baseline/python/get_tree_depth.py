"""
Load a serialized index, return max tree depth and serialization result
"""
import argparse

from R_tree_traversal import sync_traversal
from Index.Tree_generation import load_serialized_index, tree_max_depth

parser = argparse.ArgumentParser()
parser.add_argument('--tree_bin_dir', type=str, default='../tree_bin/sample_tree_level_3_self_join_19246.bin', help="dir of tree bin")
parser.add_argument('--node_bytes', type=int, default=4096, help="bytes per node")
args = parser.parse_args()
tree_bin_dir = args.tree_bin_dir
node_bytes = args.node_bytes

if __name__ == '__main__':

    print("Loading the index from disk, and join again: ")
    root_loaded = load_serialized_index(tree_bin_dir, node_bytes)
    depth = tree_max_depth(root_loaded)
    print("loaded index max depth: ", depth)
    # root_loaded.print_contents()

    # results = sync_traversal(root_loaded, root_loaded)
    # print("Result length: {}".format(len(results)))