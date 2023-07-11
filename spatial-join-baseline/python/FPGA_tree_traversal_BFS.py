import numpy as np

from Index.Region import MBR
from Index.RTree import Node, sync_traversal, join_data_nodes
from Index.Tree_generation import generate_rtree

class FPGA_tree_traversal_BFS:

    level_tree_A = None 
    level_tree_B = None
    max_level = None 

    # number of results pairs in level cache, and the pointer position
    #   of the level (current_pointer_per_level[l] <= num_pairs_per_level[l])
    num_pairs_per_level = None
    level_cache = None

    results = None

    def __init__(self, level_tree_A, level_tree_B):
        self.level_tree_A = level_tree_A 
        self.level_tree_B = level_tree_B
        self.max_level = np.amax([level_tree_A, level_tree_B])
        self.results = []

    def join_nodes_scheduler(self, root_A, root_B):
        """
        Recursively join two nodes in two R-trees. 

        Input: 
            node_A, node_B: two nodes in an R-tree
            results: the global result list of leaf intersections,
                each element of the list is a pair of object IDs
        Output: 
            None, the intersected pairs are appended in results
        """

        # The states of the scheduler
        num_pairs_per_level = dict() # number of results in layer l 
        level_cache = [] # a single big array, the starting address of each layer is recorded
        start_addr_per_layer = dict() # the starting address of results in layer l
        for i in range(1, self.max_level):
            num_pairs_per_level[i] = 0

        # put root A and root B to level 0 cache
        num_pairs_per_level[0] = 1
        start_addr_per_layer[0] = 0
        level_cache += [(root_A, root_B)]
        start_addr_per_layer[1] = 1

        current_level = 1 # the level where join happens, the results are pairs one level below
        
        for current_level in range(1, self.max_level + 1):

            start_addr_per_layer[current_level] = start_addr_per_layer[current_level - 1] + num_pairs_per_level[current_level - 1]
            # print("level: {}, num_pairs_per_level[current_level - 1]: {}".format(
            #     current_level, num_pairs_per_level[current_level - 1]))
            
            temp_results = []
            for i in range(num_pairs_per_level[current_level - 1]):
                addr = start_addr_per_layer[current_level - 1] + i
                # print("level: {}, addr {}".format(
                #     current_level, addr))
                node_A, node_B = level_cache[addr]
                temp_results += self.join_nodes(node_A, node_B)

            
            if current_level == self.max_level:
                self.results = temp_results
            else:
                level_cache += temp_results
                num_pairs_per_level[current_level] = len(temp_results)
                # print(num_pairs_per_level[current_level])

        return self.results

    def join_nodes(self, node_A, node_B):
        """
        Join two nodes.
        Output: a list of intersected pairs

        Note: for FPGA implementation, may need two types of PEs to handle this...
            one for entire page join; the other for an mbr joins a page
        """
        results = []

        if node_A.is_leaf and node_B.is_leaf: # result pairs: obj
            for i in range(node_A.get_count()):
                for j in range(node_B.get_count()):
                    if node_A.mbrs[i].intersects(node_B.mbrs[j]):
                        # here, we assume tree A and B has different object id spaces,
                        #   so the IDs need not to be different
                        results.append((node_A.obj_ids[i], node_B.obj_ids[j]))
        elif node_A.is_leaf and not node_B.is_leaf: # result pairs: A and B's children
            for j in range(node_B.get_count()):
                if node_A.mbr.intersects(node_B.mbrs[j]):
                    results.append((node_A, node_B.child_ptrs[j]))
        elif not node_A.is_leaf and node_B.is_leaf: # result pairs: A's children and B
            for i in range(node_A.get_count()):
                if node_A.mbrs[i].intersects(node_B.mbr):
                    results.append((node_A.child_ptrs[i], node_B))
        else: # result pairs: A's children and B's children
            for i in range(node_A.get_count()):
                for j in range(node_B.get_count()):
                    if node_A.mbrs[i].intersects(node_B.mbrs[j]):
                        results.append((node_A.child_ptrs[i], node_B.child_ptrs[j]))

        return results

if __name__ == '__main__':

    test_num = 100

    for i in range(test_num):
        level_tree_A = 4
        level_tree_B = 2
        root_A = generate_rtree(max_level=level_tree_A, directory_node_fanout=2, data_node_fanout=100, root_mbr=None)
        root_B = generate_rtree(max_level=level_tree_B, directory_node_fanout=2, data_node_fanout=100, root_mbr=None)

        print("A join A:")
        results = sync_traversal(root_A, root_A)
        print("Result length: {}".format(len(results)))

        print("A join B:")
        results_sync = sync_traversal(root_A, root_B)
        print("Result length: {}".format(len(results_sync)))

        print("FPGA A join B:")
        traversal_instance = FPGA_tree_traversal_BFS(level_tree_A, level_tree_B)
        results_FPGA = traversal_instance.join_nodes_scheduler(root_A, root_B)
        print("Result length: {}".format(len(results_FPGA)))
        assert len(results_FPGA) == len(results_sync)