from collections import namedtuple
import struct

class Sqlite:
    def __init__(self,command,db_file):
        self.command = command
        self.db_file = db_file

        self.BTreeHeader = namedtuple("BTreeHeader",
                                      [
                                          "type_",
                                          "free_block_start",
                                          "cell_count",
                                          "cell_start",
                                          "fragements_count",
                                          "right_pointer"
                                      ])

        self.BTreePageType = namedtuple("BTreePageType",
                                        [
                                            "BTREE_INTERIOR_INDEX",
                                            "BTREE_INTERIOR_TABLE",
                                            "BTREE_LEAF_INDEX",
                                            "BTREE_LEAF_TABLE"
                                        ])
        
        self.PageType = self.BTreePageType(0x02,0x05,0x0A,0xD)

        
    def run(self,command):
        self.db_file.seek(16)
        self.page_size = int.from_bytes(self.db_file.read(2), byteorder="big")

        self.db_file.seek(0)

        page = self.db_file.read(self.page_size)

        b_tree_header = self.parse_btree_header(page,True)

        print(f"database page size:{self.page_size}")
        print(f"number of tables:{b_tree_header.cell_count}")
        
    def parse_btree_header(self,page,is_firstpage = False):
        offset = 100 if is_firstpage else 0

        (type_, free_block_start, cell_count, cell_start,fragments_count,) = struct.unpack_from(">BHHHB",page, offset)

        if type_ in (self.PageType.BTREE_INTERIOR_INDEX, self.PageType.BTREE_INTERIOR_TABLE):
            (right_pointer,) = struct.unpack_from(">I",page,offset+8)

        else:
            right_pointer = 0 

        return self.BTreeHeader(type_,free_block_start,cell_count,cell_start or 65536,fragments_count,right_pointer)

    
             





            