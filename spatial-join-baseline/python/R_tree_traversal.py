"""
This file reproduces the R-tree synchronized traversal process by
  recursive function call (the CPU style)
"""

import numpy as np

from Index.Region import MBR
from Index.RTree import Node, sync_traversal, join_data_nodes
from Index.Tree_generation import generate_rtree


def create_nodes():
    print("\n\nRunning self join on a single node:")
    node = Node(node_id=0, is_leaf=True, mbr=MBR(1,2,1,2))
    node.add_entry(MBR(1,2,1,2), 10)
    mbr_list = [MBR(1,2,1,2)] * 5
    node.add_entries(mbr_list, [1,2,3,4,5])
    node.print_contents()

    results = []
    results = sync_traversal(node, node)
    print("self join results ({} pairs): {}".format(len(results), results))
    results = join_data_nodes(node, node)
    print("self join results ({} pairs): {}".format(len(results), results))

    node = Node(node_id=0, is_leaf=False, mbr=MBR(3,4,3,4))
    node.add_entry(MBR(3,4,3,4), 12)
    mbr_list = [MBR(3,4,3,4)] * 5
    node.add_entries(mbr_list, [6,7,8,9,10])
    node.print_contents()

def join_trees():
    print("\n\nRunning synchronous traversal on generated R trees:")
    root_A = generate_rtree(max_level=4, directory_node_fanout=2, data_node_fanout=100, root_mbr=None)
    root_B = generate_rtree(max_level=2, directory_node_fanout=2, data_node_fanout=100, root_mbr=None)

    print("A join A:")
    results = sync_traversal(root_A, root_A)
    print("Result length: {}".format(len(results)))

    print("A join B:")
    results = sync_traversal(root_A, root_B)
    print("Result length: {}".format(len(results)))


if __name__ == '__main__':
    create_nodes()
    join_trees() 