import os
import struct
import bisect
from typing import List

PAGE_SIZE = 4096  # 4KB页大小，与多数文件系统匹配


class Page:
    """页基类"""
    def __init__(self, order: int, page_id: int):
        self.page_id = page_id
        self.order = order
        self.is_leaf = False
        self.keys = []
        self.children = []  # 对于叶子页存储值，内部页存储子页号

    def serialize(self) -> bytes:
        """序列化页数据"""
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: bytes, order: int, page_id: int):
        """反序列化页数据"""
        header = struct.unpack('<BHI', data[:7])
        is_leaf = header[0]
        key_count = header[1]
        next_page = header[2]
        
        if is_leaf:
            page = LeafPage(order, page_id)
            page.next_page = next_page
        else:
            page = InternalPage(order, page_id)
        
        # 解析键和子页/值
        offset = 7
        for _ in range(key_count):
            key_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            key = data[offset:offset+key_len]
            offset += key_len
            page.keys.append(key)
            
            if is_leaf:
                value = struct.unpack('<I', data[offset:offset+4])[0]
                offset += 4
                page.children.append(value)
            else:
                child = struct.unpack('<I', data[offset:offset+4])[0]
                offset += 4
                page.children.append(child)
        
        if not is_leaf and key_count > 0:
            # 内部页最后一个子页
            last_child = struct.unpack('<I', data[offset:offset+4])[0]
            page.children.append(last_child)
        
        return page

    def is_overfull(self) -> bool:
        """检查页是否已满"""
        return len(self.keys) >= self.order


class LeafPage(Page):
    """叶子页"""
    def __init__(self, order: int, page_id: int):
        super().__init__(order, page_id)
        self.is_leaf = True
        self.next_page = 0  # 下一页指针

    def serialize(self) -> bytes:
        header = struct.pack('<BHI', 
            1,  # is_leaf
            len(self.keys),
            self.next_page
        )
        
        data = bytearray(header)
        for key, value in zip(self.keys, self.children):
            key_data = struct.pack('<H', len(key)) + key
            value_data = struct.pack('<I', value)
            data.extend(key_data + value_data)
        
        return bytes(data)

    def add_record(self, key: bytes, value: int) -> bool:
        """添加记录到叶子页"""
        pos = bisect.bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return False  # 键已存在
        
        self.keys.insert(pos, key)
        self.children.insert(pos, value)
        return True

    def get_values(self, key: bytes) -> List[int]:
        """获取键对应的值"""
        pos = bisect.bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return [self.children[pos]]
        return []

class InternalPage(Page):
    """内部页"""
    def serialize(self) -> bytes:
        header = struct.pack('<BHI', 
            0,  # is_leaf
            len(self.keys),
            0   # 占位
        )
        
        data = bytearray(header)
        for key, child in zip(self.keys, self.children[:-1]):
            key_data = struct.pack('<H', len(key)) + key
            child_data = struct.pack('<I', child)
            data.extend(key_data + child_data)
        
        # 最后一个子页
        if self.children:
            data.extend(struct.pack('<I', self.children[-1]))
        
        return bytes(data)


class BPlusTreeIndex:
    """
    支持磁盘持久化的B+树索引实现
    """
    def __init__(self, filename: str, order: int = 100):
        self.filename = filename
        self.order = order
        self.metadata = {
            'magic': 0x13579BDF,
            'root_page': 1,  # 从1开始，0为元数据页
            'free_pages': []
        }
        
        # 初始化或加载现有索引
        if not os.path.exists(filename):
            self._initialize_new_file()
        else:
            self._load_metadata()

    def _initialize_new_file(self):
        """初始化新索引文件"""
        with open(self.filename, 'wb') as f:
            # 元数据页
            meta_page = struct.pack('<IIQI', 
                self.metadata['magic'],
                self.order,
                self.metadata['root_page'],
                0  # free_pages_count
            ).ljust(PAGE_SIZE, b'\0')
            f.write(meta_page)
            
            # 初始化根叶子页
            root_page = LeafPage(self.order, 1)
            f.write(root_page.serialize().ljust(PAGE_SIZE, b'\0'))

    def _load_metadata(self):
        """加载元数据"""
        with open(self.filename, 'rb') as f:
            data = f.read(PAGE_SIZE)
            magic, order, root_page, free_count = struct.unpack('<IIQI', data[:20])
            if magic != self.metadata['magic']:
                raise ValueError("Invalid index file format")
            self.order = order
            self.metadata['root_page'] = root_page
            
            if free_count > 0:
                free_pages = struct.unpack(f'<{free_count}I', data[20:20+4*free_count])
                self.metadata['free_pages'] = list(free_pages)

    def _get_file_size(self) -> int:
        """获取文件大小"""
        return os.path.getsize(self.filename)

    def _get_page(self, page_id: int) -> Page:
        """从磁盘加载页"""
        with open(self.filename, 'rb') as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
            return Page.deserialize(data, self.order, page_id)

    def _write_page(self, page: Page):
        """写回页到磁盘"""
        with open(self.filename, 'r+b') as f:
            f.seek(page.page_id * PAGE_SIZE)
            data = page.serialize()
            f.write(data.ljust(PAGE_SIZE, b'\0'))

    def _find_leaf_page(self, key: bytes, start_page: Page = None, path: List[Page] = None) -> Page:
        """查找包含key的叶子页"""
        if start_page is None:
            start_page = self._get_page(self.metadata['root_page'])
        
        current = start_page
        if path is not None:
            path.clear()
            
        while not current.is_leaf:
            if path is not None:
                path.append(current)
            
            pos = bisect.bisect_right(current.keys, key)
            current = self._get_page(current.children[pos])
            
        return current

    def _allocate_page(self, is_leaf: bool) -> Page:
        """分配新页"""
        if self.metadata['free_pages']:
            page_id = self.metadata['free_pages'].pop()
        else:
            page_id = self._get_file_size() // PAGE_SIZE
        
        return LeafPage(self.order, page_id) if is_leaf else InternalPage(self.order, page_id)

    def insert(self, key: bytes, value: int):
        """插入键值对"""
        root_page = self._get_page(self.metadata['root_page'])
        path = []
        leaf = self._find_leaf_page(key, root_page, path)
        
        if leaf.add_record(key, value):
            self._write_page(leaf)
            if leaf.is_overfull():
                self._split_leaf(leaf, path)

    def search(self, key: bytes) -> List[int]:
        """查找键对应的值"""
        leaf = self._find_leaf_page(key)
        return leaf.get_values(key)

    def range_query(self, start: bytes, end: bytes) -> List[int]:
        """范围查询"""
        results = []
        current_page = self._find_leaf_page(start)
        
        while current_page:
            for key, value in zip(current_page.keys, current_page.children):
                if start <= key <= end:
                    results.append(value)
                elif key > end:
                    return results
            
            if current_page.next_page == 0:
                break
            current_page = self._get_page(current_page.next_page)
        
        return results

    def _split_leaf(self, leaf: LeafPage, path: List[Page]):
        """叶子页分裂"""
        new_leaf = self._allocate_page(is_leaf=True)
        mid = len(leaf.keys) // 2
        
        # 分裂键值
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.children = leaf.children[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.children = leaf.children[:mid]
        
        # 更新链表
        new_leaf.next_page = leaf.next_page
        leaf.next_page = new_leaf.page_id
        
        # 写回页面
        self._write_page(leaf)
        self._write_page(new_leaf)
        
        # 提升键到父节点
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf, path)

    def _split_internal(self, node: InternalPage, path: List[Page]):
        """内部页分裂"""
        new_node = self._allocate_page(is_leaf=False)
        mid = len(node.keys) // 2
        
        # 分裂键和子页
        new_node.keys = node.keys[mid+1:]
        new_node.children = node.children[mid+1:]
        mid_key = node.keys[mid]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid+1]
        
        # 写回页面
        self._write_page(node)
        self._write_page(new_node)
        
        # 提升键到父节点
        self._insert_into_parent(node, mid_key, new_node, path)

    def _insert_into_parent(self, left: Page, key: bytes, right: Page, path: List[Page]):
        """向父节点插入分裂后的键"""
        if not path:
            # 创建新根
            new_root = self._allocate_page(is_leaf=False)
            new_root.keys = [key]
            new_root.children = [left.page_id, right.page_id]
            self.metadata['root_page'] = new_root.page_id
            self._write_page(new_root)
            
            # 更新元数据
            with open(self.filename, 'r+b') as f:
                f.seek(8)  # 跳过magic和order
                f.write(struct.pack('<Q', new_root.page_id))
        else:
            parent = path.pop()
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos+1, right.page_id)
            
            self._write_page(parent)
            
            if parent.is_overfull():
                self._split_internal(parent, path)

# 测试代码
if __name__ == "__main__":
    # 创建或打开索引
    if os.path.exists("test.idx"):
        os.remove("test.idx")
    index = BPlusTreeIndex("test.idx", order=4)
    
    # 插入测试数据
    test_data = [
        (b"Alice", 1001),
        (b"Bob", 1002),
        (b"Charlie", 1003),
        (b"David", 1004),
        (b"Eve", 1005),
        (b"Frank", 1006),
        (b"George", 1007),
    ]
    
    print("插入测试数据...")
    for key, value in test_data:
        index.insert(key, value)
        
    # 查询测试
    print("\n查询测试:")
    for key, _ in test_data:
        result = index.search(key)
        print(f"查询 {key.decode()}: {result}")
    
    # 范围查询测试
    print("\n范围查询测试:")
    range_result = index.range_query(b"Bob", b"Eve")
    print(f"范围 Bob-Eve: {range_result}")
    
