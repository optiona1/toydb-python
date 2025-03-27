class BPlusTree:
    """
    B+树实现类，支持插入、查找、删除操作。
    
    属性:
        order (int): B+树的阶数，决定每个节点的键数量上限。
        root (Node): 树的根节点，可以是内部节点或叶子节点。
    """
    def __init__(self, order=3):
        self.order = order
        self.root = LeafNode(order)  # 初始时根节点为叶子节点

    def insert(self, key, value):
        """
        插入键值对到B+树中。
        
        参数:
            key: 要插入的键。
            value: 要插入的值。
        """
        leaf, path = self._find_leaf_with_path(key)
        leaf.insert(key, value)
        if leaf.is_overfilled():
            self._handle_split(leaf, path)

    def get(self, key):
        """
        根据键查找对应的值。
        
        参数:
            key: 要查找的键。
            
        返回:
            对应的值，若键不存在则返回None。
        """
        leaf = self._find_leaf(key)
        return leaf.get(key)

    def delete(self, key):
        """
        从B+树中删除键及其对应的值。
        
        参数:
            key: 要删除的键。
        """
        leaf, path = self._find_leaf_with_path(key)
        if leaf.delete(key):
            self._handle_underflow(leaf, path)

    def _find_leaf(self, key):
        """
        根据键找到对应的叶子节点。
        
        参数:
            key: 要查找的键。
            
        返回:
            包含该键的叶子节点。
        """
        current = self.root
        while not current.is_leaf:
            current = current.get_child(key)
        return current

    def _find_leaf_with_path(self, key):
        """
        根据键找到叶子节点，并记录查找路径。
        
        参数:
            key: 要查找的键。
            
        返回:
            tuple: (叶子节点, 路径列表)
        """
        path = []
        current = self.root
        while not current.is_leaf:
            path.append(current)
            current = current.get_child(key)
        return current, path

    def _handle_split(self, node, path):
        """
        处理节点分裂。
        
        参数:
            node: 发生分裂的节点。
            path: 到该节点的路径。
        """
        if node.is_leaf:
            new_node, promote_key = node.split()
        else:
            new_node, promote_key = node.split()
        
        if not path:  # 分裂的是根节点
            new_root = InternalNode(self.order)
            new_root.keys = [promote_key]
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            parent = path[-1]
            parent.insert_child(promote_key, new_node)
            if parent.is_overfilled():
                self._handle_split(parent, path[:-1])

    def _handle_underflow(self, node, path):
        """
        处理节点下溢（键数量不足）。
        
        参数:
            node: 发生下溢的节点。
            path: 到该节点的路径。
        """
        if node.is_root() or node.is_half_filled():
            return
        
        parent = path[-1]
        left_sib, right_sib = parent.get_siblings(node)
        
        # 尝试从左兄弟借
        if left_sib and left_sib.can_lend():
            parent.redistribute(left_sib, node, is_left=True)
        # 尝试从右兄弟借
        elif right_sib and right_sib.can_lend():
            parent.redistribute(node, right_sib, is_left=False)
        # 需要合并
        else:
            if left_sib:
                parent.merge(left_sib, node)
            else:
                parent.merge(node, right_sib)
            
            if parent.is_underfilled():
                self._handle_underflow(parent, path[:-1])

class Node:
    """节点基类，定义公共接口"""
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.is_leaf = False

    def is_root(self):
        return self == bplus_tree.root

    def is_overfilled(self):
        return len(self.keys) > self.order

    def is_underfilled(self):
        return len(self.keys) < (self.order + 1) // 2 - 1

    def is_half_filled(self):
        return len(self.keys) >= (self.order + 1) // 2


class LeafNode(Node):
    """叶子节点类"""
    def __init__(self, order):
        super().__init__(order)
        self.values = []
        self.next_leaf = None
        self.is_leaf = True

    def insert(self, key, value):
        # 找到插入位置并保持有序
        index = 0
        while index < len(self.keys) and self.keys[index] < key:
            index += 1
        self.keys.insert(index, key)
        self.values.insert(index, value)

    def get(self, key):
        # 使用二分查找提高效率
        left, right = 0, len(self.keys)
        while left < right:
            mid = (left + right) // 2
            if self.keys[mid] < key:
                left = mid + 1
            else:
                right = mid
        if left < len(self.keys) and self.keys[left] == key:
            return self.values[left]
        return None

    def delete(self, key):
        if key not in self.keys:
            return False
        index = self.keys.index(key)
        self.keys.pop(index)
        self.values.pop(index)
        return True

    def split(self):
        # 创建新节点并转移数据
        new_node = LeafNode(self.order)
        mid = len(self.keys) // 2
        new_node.keys = self.keys[mid:]
        new_node.values = self.values[mid:]
        # 更新链表指针
        new_node.next_leaf = self.next_leaf
        self.next_leaf = new_node
        # 返回新节点和提升的键
        return new_node, new_node.keys[0]


class InternalNode(Node):
    """内部节点类"""
    def __init__(self, order):
        super().__init__(order)
        self.children = []

    def get_child(self, key):
        # 根据键找到合适的子节点
        index = 0
        while index < len(self.keys) and key >= self.keys[index]:
            index += 1
        return self.children[index]

    def insert_child(self, key, child_node):
        # 找到插入位置并保持有序
        index = 0
        while index < len(self.keys) and self.keys[index] < key:
            index += 1
        self.keys.insert(index, key)
        self.children.insert(index + 1, child_node)

    def split(self):
        # 创建新节点并转移数据
        new_node = InternalNode(self.order)
        mid = len(self.keys) // 2
        promote_key = self.keys[mid]
        # 新节点获取分裂后的键和子节点
        new_node.keys = self.keys[mid + 1:]
        new_node.children = self.children[mid + 1:]
        # 原节点保留剩余数据
        self.keys = self.keys[:mid]
        self.children = self.children[:mid + 1]
        return new_node, promote_key

    def get_siblings(self, child):
        # 获取相邻兄弟节点
        index = self.children.index(child)
        left = self.children[index - 1] if index > 0 else None
        right = self.children[index + 1] if index < len(self.children)-1 else None
        return left, right

    def redistribute(self, from_node, to_node, is_left):
        # 重新分配键和子节点
        if is_left:
            key_index = self.children.index(from_node)
            key = self.keys[key_index]
            # 从左兄弟借最后一个元素
            last_key = from_node.keys.pop()
            last_child = from_node.children.pop()
            # 更新当前节点
            to_node.keys.insert(0, key)
            to_node.children.insert(0, last_child)
            # 更新父节点键
            self.keys[key_index] = last_key
        else:
            key_index = self.children.index(to_node)
            key = self.keys[key_index]
            # 从右兄弟借第一个元素
            first_key = to_node.keys.pop(0)
            first_child = to_node.children.pop(0)
            # 更新当前节点
            from_node.keys.append(key)
            from_node.children.append(first_child)
            # 更新父节点键
            self.keys[key_index] = first_key

    def merge(self, left, right):
        # 合并两个子节点
        index = self.children.index(left)
        # 将右节点的数据合并到左节点
        left.keys += [self.keys[index]] + right.keys
        left.children += right.children
        # 删除父节点中的键和子节点
        self.keys.pop(index)
        self.children.pop(index + 1)
        # 如果是叶子节点需要维护链表
        if left.is_leaf:
            left.next_leaf = right.next_leaf

# 示例用法
if __name__ == "__main__":
    bplus_tree = BPlusTree(order=3)
    
    # 插入测试数据
    data = [(i, f"value{i}") for i in range(10)]
    for key, value in data:
        bplus_tree.insert(key, value)
    
    # 查找测试
    print(bplus_tree.get(5))  # 输出: value5
    print(bplus_tree.get(15))  # 输出: None
    
    # 删除测试
    bplus_tree.delete(5)
    print(bplus_tree.get(5))  # 输出: None

