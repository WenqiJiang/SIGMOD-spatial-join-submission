
class Node:
    """
    Refer to R-tree Node: https://github.com/WenqiJiang/CPU-spatial-join/blob/main/include/2D/point_index/RTree.h
    """
    node_id = None 
    is_leaf = False
    count = 0
    mbr = None # MBR for the entire node

    child_ptrs = None # for directory nodes, each element in the list is a node
    obj_ids = None # for data nodes
    mbrs = None

    def __init__(self, node_id : int, is_leaf : bool, mbr):
        self.node_id = node_id 
        self.is_leaf = is_leaf
        self.mbr = mbr 
        if not is_leaf:
            self.child_ptrs = [] # int list
        else:
            self.obj_ids = [] # int list
        self.mbrs = []

    def print_contents(self):
        print("Is leaf: ", self.is_leaf)
        print("Number of items: ", self.count)
        print("MBR of the node: ", self.mbr)
        print("Children pointers: ", self.child_ptrs)
        print("Object IDs: ", self.obj_ids)
        print("Children MBR list: ")
        for mbr in self.mbrs:
            mbr.print()

    def get_count(self):
        return self.count

    def add_entry(self, mbr, data):
        if not self.is_leaf:
            self.child_ptrs.append(data)
        else:
            self.obj_ids.append(data)
        self.mbrs.append(mbr)
        self.count += 1

    def add_entries(self, mbrs : list, data: list):
        assert len(mbrs) == len(data)
        if not self.is_leaf:
            for d in data:
                self.child_ptrs.append(d)
        else:
            for d in data:
                self.obj_ids.append(d)
        for mbr in mbrs:
            self.mbrs.append(mbr)
        self.count += len(data)

    def deep_copy(self):
        new_node = Node(node_id=self.node_id, is_leaf=self.is_leaf, mbr=self.mbr)
        if self.is_leaf:
            new_node.add_entries(self.mbrs, self.obj_ids)
        else: 
            # recursively call deep copy on the children
            for child_ptr in self.child_ptrs:
                new_child = child_ptr.deep_copy()
                new_node.add_entry(new_child.mbr, new_child)
        return new_node

def sync_traversal(root_A, root_B):
    """
    Synchronous traversal on R-Tree
    """
    results = []
    join_nodes_recursive(root_A, root_B, results)

    return results

def join_nodes_recursive(node_A, node_B, results: list):
    """
    Recursively join two nodes in two R-trees. 

    Input: 
        node_A, node_B: two nodes in an R-tree
        results: the global result list of leaf intersections,
            each element of the list is a pair of object IDs
    Output: 
        None, the intersected pairs are appended in results
    """
    
    # One of the nodes is leaf
    if node_A.is_leaf and not node_B.is_leaf:
        for i in range(node_B.get_count()):
            join_nodes_recursive(node_A, node_B.child_ptrs[i], results)
    elif not node_A.is_leaf and node_B.is_leaf:
        for i in range(node_A.get_count()):
            join_nodes_recursive(node_B, node_A.child_ptrs[i], results)
    elif node_A.is_leaf and node_B.is_leaf: # both are leaf
        for i in range(node_A.get_count()):
            for j in range(node_B.get_count()):
                if node_A.mbrs[i].intersects(node_B.mbrs[j]):
                    # here, we assume tree A and B has different object id spaces,
                    #   so the IDs need not to be different
                    results.append((node_A.obj_ids[i], node_B.obj_ids[j]))
    else: # neither is leaf
        for i in range(node_A.get_count()):
            for j in range(node_B.get_count()):
                if node_A.mbrs[i].intersects(node_B.mbrs[j]):
                    join_nodes_recursive(node_A.child_ptrs[i], node_B.child_ptrs[j], results)

def join_data_nodes(node_A, node_B):
    """
    Join two data nodes.
    Iput: Both nodes are leaf nodes. 
    Output: a list of intersected pairs
    """
    assert node_A.is_leaf and node_B.is_leaf 

    results = []
    for i in range(node_A.get_count()):
        for j in range(node_B.get_count()):
            if node_A.mbrs[i].intersects(node_B.mbrs[j]):
                if node_A.obj_ids[i] != node_B.obj_ids[j]:
                    results.append((node_A.obj_ids[i], node_B.obj_ids[j]))
    return results
