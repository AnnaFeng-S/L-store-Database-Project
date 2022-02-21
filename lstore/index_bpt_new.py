"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from xmlrpc.client import MAXINT
import math

from sklearn.model_selection import learning_curve

class Node:
    def __init__(self, order):
        self.order = order
        self.values = []
        self.keys = []
        self.nextKey = None
        self.parent = None
        self.has_leaf = False

    def insert_node(self, leaf, value, key):
        if (leaf.values):
            for i,v in enumerate(leaf.values):
                if (value == v):
                    leaf.keys[i].append(key)
                    break
                elif (value < v):
                    leaf.values = leaf.values[:i] + [value] + leaf.values[i:]
                    leaf.keys = leaf.keys[:i] + [[key]] + leaf.keys[i:]
                    break
                elif (i + 1 == len(leaf.values)):
                    leaf.values.append(value)
                    leaf.keys.append([key])
                    break
        else:
            leaf.values = [value]
            leaf.keys = [[key]]


class BPT:
    def __init__(self, order):
        self.root = Node(order)
        self.root.has_leaf = True

    def insert(self, value, key):
        old_node = self.search(value)
        old_node.insert_node(old_node, value, key)

        if (len(old_node.values) == old_node.order):
            node1 = Node(old_node.order)
            node1.has_leaf = True
            node1.parent = old_node.parent
            mid = int(math.ceil(old_node.order / 2)) - 1
            node1.values = old_node.values[mid + 1:]
            node1.keys = old_node.keys[mid + 1:]
            node1.nextKey = old_node.nextKey
            old_node.values = old_node.values[:mid + 1]
            old_node.keys = old_node.keys[:mid + 1]
            old_node.nextKey = node1
            self.insert_in_parent(old_node, node1.values[0], node1)

    def search(self, value):
        current_node = self.root
        while(not current_node.has_leaf):
            for i,v in enumerate(current_node.values):
                if (value == v):
                    current_node = current_node.keys[i + 1]
                    break
                elif (value < v):
                    current_node = current_node.keys[i]
                    break
                elif (i + 1 == len(current_node.values)):
                    current_node = current_node.keys[i + 1]
                    break

        return current_node

    """def find(self, value, key):
        l = self.search(value)
        for i, item in enumerate(l.values):
            if item == value:
                if key in l.keys[i]:
                    return True
                else:
                    return False
        return False"""
    
    def location(self, value):
        l = self.search(value)
        loc = 0
        for i,v in enumerate(l.values):
            if v == value:
                loc = i
                break
        if (value in l.values):
            return l.keys[loc]
        else:
            return None
    
    def range(self, begin, end):
        l = self.search(begin)
        start = -1
        last = -1
        rids = []
        while (last == -1):
            for i,v in enumerate(l.values):
                if v == begin:
                    start = i
                    rids += l.keys[i]
                elif start != -1:
                    rids += l.keys[i]
                if v == end:
                    last = v
                    break
            l = l.nextKey
        return rids

    def insert_in_parent(self, old_node, value, new_node):
        if (self.root == old_node):
            rootNode = Node(old_node.order)
            rootNode.values = [value]
            rootNode.keys = [old_node, new_node]
            self.root = rootNode
            old_node.parent = rootNode
            new_node.parent = rootNode
            return

        parentNode = old_node.parent
        children = parentNode.keys
        for i in range(len(children)):
            if (children[i] == old_node):
                parentNode.values = parentNode.values[:i] + [value] + parentNode.values[i:]
                parentNode.keys = parentNode.keys[:i + 1] + [new_node] + parentNode.keys[i + 1:]
                if (len(parentNode.keys) > parentNode.order):
                    new_parent = Node(parentNode.order)
                    new_parent.parent = parentNode.parent
                    mid = int(math.ceil(parentNode.order / 2)) - 1
                    new_parent.values = parentNode.values[mid + 1:]
                    new_parent.keys = parentNode.keys[mid + 1:]
                    value_ = parentNode.values[mid]
                    if (mid == 0):
                        parentNode.values = parentNode.values[:mid + 1]
                    else:
                        parentNode.values = parentNode.values[:mid]
                    parentNode.keys = parentNode.keys[:mid + 1]
                    for j in parentNode.keys:
                        j.parent = parentNode
                    for j in new_parent.keys:
                        j.parent = new_parent
                    self.insert_in_parent(parentNode, value_, new_parent)

    def delete(self, value, key):
        node_ = self.search(value)

        check = False
        for i, v in enumerate(node_.values):
            if v == value:
                check = True

                if key in node_.keys[i]:
                    if len(node_.keys[i]) > 1:
                        node_.keys[i].pop(node_.keys[i].index(key))
                    elif node_ == self.root:
                        node_.values.pop(i)
                        node_.keys.pop(i)
                    else:
                        node_.keys[i].pop(node_.keys[i].index(key))
                        del node_.keys[i]
                        node_.values.pop(node_.values.index(value))
                        self.deleteEntry(node_, value, key)
                else:
                    print("Value not in Key")
                    return
        if check == False:
            print("Value not in Tree")
            return

    def deleteEntry(self, node_, value, key):

        if not node_.has_leaf:
            for i, item in enumerate(node_.keys):
                if item == key:
                    node_.keys.pop(i)
                    break
            for i, item in enumerate(node_.values):
                if item == value:
                    node_.values.pop(i)
                    break

        if self.root == node_ and len(node_.keys) == 1:
            self.root = node_.keys[0]
            node_.keys[0].parent = None
            del node_
            return
        elif (len(node_.keys) < int(math.ceil(node_.order / 2)) and node_.has_leaf == False) or (len(node_.values) < int(math.ceil((node_.order - 1) / 2)) and node_.has_leaf == True):

            is_predecessor = 0
            parentNode = node_.parent
            PrevNode = -1
            NextNode = -1
            PrevK = -1
            PostK = -1
            for i, item in enumerate(parentNode.keys):

                if item == node_:
                    if i > 0:
                        PrevNode = parentNode.keys[i - 1]
                        PrevK = parentNode.values[i - 1]

                    if i < len(parentNode.keys) - 1:
                        NextNode = parentNode.keys[i + 1]
                        PostK = parentNode.values[i]

            if PrevNode == -1:
                ndash = NextNode
                value_ = PostK
            elif NextNode == -1:
                is_predecessor = 1
                ndash = PrevNode
                value_ = PrevK
            else:
                if len(node_.values) + len(NextNode.values) < node_.order:
                    ndash = NextNode
                    value_ = PostK
                else:
                    is_predecessor = 1
                    ndash = PrevNode
                    value_ = PrevK

            if len(node_.values) + len(ndash.values) < node_.order:
                if is_predecessor == 0:
                    node_, ndash = ndash, node_
                ndash.keys += node_.keys
                if not node_.has_leaf:
                    ndash.values.append(value_)
                else:
                    ndash.nextKey = node_.nextKey
                ndash.values += node_.values

                if not ndash.has_leaf:
                    for j in ndash.keys:
                        j.parent = ndash

                self.deleteEntry(node_.parent, value_, node_)
                del node_
            else:
                if is_predecessor == 1:
                    if not node_.has_leaf:
                        ndashpm = ndash.keys.pop(-1)
                        ndashkm_1 = ndash.values.pop(-1)
                        node_.keys = [ndashpm] + node_.keys
                        node_.values = [value_] + node_.values
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.values):
                            if item == value_:
                                p.values[i] = ndashkm_1
                                break
                    else:
                        ndashpm = ndash.keys.pop(-1)
                        ndashkm = ndash.values.pop(-1)
                        node_.keys = [ndashpm] + node_.keys
                        node_.values = [ndashkm] + node_.values
                        parentNode = node_.parent
                        for i, item in enumerate(p.values):
                            if item == value_:
                                parentNode.values[i] = ndashkm
                                break
                else:
                    if not node_.has_leaf:
                        ndashp0 = ndash.keys.pop(0)
                        ndashk0 = ndash.values.pop(0)
                        node_.keys = node_.keys + [ndashp0]
                        node_.values = node_.values + [value_]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.values):
                            if item == value_:
                                parentNode.values[i] = ndashk0
                                break
                    else:
                        ndashp0 = ndash.keys.pop(0)
                        ndashk0 = ndash.values.pop(0)
                        node_.keys = node_.keys + [ndashp0]
                        node_.values = node_.values + [ndashk0]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.values):
                            if item == value_:
                                parentNode.values[i] = ndash.values[0]
                                break

                if not ndash.has_leaf:
                    for j in ndash.keys:
                        j.parent = ndash
                if not node_.has_leaf:
                    for j in node_.keys:
                        j.parent = node_
                if not parentNode.has_leaf:
                    for j in parentNode.keys:
                        j.parent = parentNode



record_len = 6
bplustree = BPT(record_len)
bplustree.insert(5, 1)
bplustree.insert(5, 2)
bplustree.delete(5, 2)

bplustree.insert(6, 2)
bplustree.insert(7, 3)
bplustree.insert(8, 4)
bplustree.insert(9, 5)

bplustree.insert(10, 6)
bplustree.insert(11, 7)
bplustree.insert(12, 8)
bplustree.insert(12, 8)

bplustree.delete(12, 8)
bplustree.delete(12, 8)
bplustree.delete(11, 7)
bplustree.delete(10, 6)
bplustree.delete(8, 4)



print(bplustree.range(6,9))
print(bplustree.location(5))
print(bplustree.location(7))
print(bplustree.location(10))

class Index:
    
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        # rid count
        self.indices[table.key] = {}

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        try:
            return self.indices[column].location(value)
        except:
            return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        col_dict = self.indices[column]
        if col_dict is None:
            return None
        else:
            return col_dict.range(begin,end)
        

    """
    # optional: Create index on specific column
    """

    def create_index(self, column):
        # The maximum number of keys in a record
        record_len = 100;
        self.indices[column] = BPT(record_len)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        self.indices[column] = None
        
    """ 
    # Insert New Record
    """
    def insert(self, column, value, rid):
        col_dict = self.indices[column]
        if col_dict is not None:
            col_dict.insert(value,rid)

    """ 
    # Delete Record
    """
    def delete(self, column, value, rid):
        col_dict = self.indices[column]
        if col_dict.search(value):
            col_dict.delete(value,rid)
            

    """ 
    # Update record with this
    """
    def update(self, column, old_value, new_value, rid):
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)
