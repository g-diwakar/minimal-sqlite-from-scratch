from collections import namedtuple
import struct

class Sqlite:
    def __init__(self,db_file):
        self.db_file = db_file

        self.BTreeHeader = namedtuple("BTreeHeader",
                                      [
                                          "type_",
                                          "free_block_start",
                                          "cell_count",
                                          "cell_start",
                                          "fragements_count",
                                          "right_pointer",
                                          "bytes_read" #extra info
                                      ])

        self.BTreePageType = namedtuple("BTreePageType",
                                        [
                                            "BTREE_INTERIOR_INDEX",
                                            "BTREE_INTERIOR_TABLE",
                                            "BTREE_LEAF_INDEX",
                                            "BTREE_LEAF_TABLE"
                                        ])
        
        self.PageType = self.BTreePageType(0x02,0x05,0x0A,0xD)

        self.db_file.seek(16)
        self.page_size = int.from_bytes(self.db_file.read(2), byteorder="big")
        self.db_file.seek(0)

        self.command_mapper = {".dbinfo":self.run_dbinfo, ".tables":self.run_tables}

    def run(self,command):
        # self.db_file.seek(16)
        # self.page_size = int.from_bytes(self.db_file.read(2), byteorder="big")

        # self.db_file.seek(0)

        # page = self.db_file.read(self.page_size)

        # b_tree_header = self.parse_btree_header(page,True)

        # print(f"database page size: {self.page_size}")
        # print(f"number of tables: {b_tree_header.cell_count}")

        self.command_mapper[command].__call__()

    
    def run_dbinfo(self):
        page = self.db_file.read(self.page_size)
        
        b_tree_header = self.parse_btree_header(page,True)
        
        print(f"database page size: {self.page_size}")
        print(f"number of tables: {b_tree_header.cell_count}")

    def run_tables(self):
        
        #get the encoding types
        self.db_file.seek(56)
        text_encoding = ["UTF8","UTF16LE","UTF16BE"] [int.from_bytes(self.db_file.read(4), byteorder="big")-1]

        #get the page
        self.db_file.seek(0)
        page = self.db_file.read(self.page_size)
        
        
        b_tree_header = self.parse_btree_header(page,True)

        b_tree_offset = 100 + b_tree_header.bytes_read

        cells = []

        for i in range(b_tree_header.cell_count):
            (cell_content_offset,) = struct.unpack_from(">H", page, b_tree_offset)

            b_tree_offset += 2

            _payload_size , bytes_read = self.parse_varint(page,cell_content_offset)
            cell_content_offset += bytes_read

            row_id, bytes_read = self.parse_varint(page,cell_content_offset)
            cell_content_offset += bytes_read

            column_values, bytes_read = self.parse_record(page,cell_content_offset, text_encoding)

            column_values.insert(0,row_id)

            cells.append(column_values)


        print(" ".join(cell[3] for cell in cells if cell[1] =="table" and not cell[3].startswith("sqlite_")))

        
    def parse_btree_header(self,page,is_firstpage = False):
        offset = 100 if is_firstpage else 0

        (type_, free_block_start, cell_count, cell_start,fragments_count,) = struct.unpack_from(">BHHHB",page, offset)

        if type_ in (self.PageType.BTREE_INTERIOR_INDEX, self.PageType.BTREE_INTERIOR_TABLE):
            (right_pointer,) = struct.unpack_from(">I",page,offset+8)
            bytes_read = 12

        else:
            right_pointer = 0 
            bytes_read = 8

        return self.BTreeHeader(type_,free_block_start,cell_count,cell_start or 65536,fragments_count,right_pointer,bytes_read)

    
    def parse_varint(self,buffer, offset=0):
        n = 0 
        
        for i in range(offset, offset+9):
            byte = buffer[i]

            if byte & 0x80 == 0:
                n<<=8
                n |= byte
                break 
            else:
                n<<=7
                n |= byte & 0x7F
        
        else:
            i-=1
        
        return n, i+1 - offset

    def parse_record(self,buffer,offset, text_encoding):
        initial_offset = offset

        header_size, bytes_read = self.parse_varint(buffer, offset)
        header_end = offset + header_size

        offset += bytes_read

        column_types = []

        while offset != header_end:
            column_serial_type, bytes_read = self.parse_varint(buffer, offset)
            column_types.append(column_serial_type)
            offset += bytes_read

        column_values = []

        for column_serial_type in column_types:

            if column_serial_type == 0:
                column_values.append(None)

            elif 1 <= column_serial_type <= 6:
                number_byte_size = (
                    column_serial_type
                    if column_serial_type < 5
                    else 6
                    if column_serial_type == 5
                    else 8
                )
                value = int.from_bytes(
                    buffer[offset : offset + number_byte_size], byteorder="big", signed=True
                )
                column_values.append(value)
                offset += number_byte_size

            elif column_serial_type == 7:
                value = struct.unpack_from(">d", buffer, offset)
                column_values.append(value)
                offset += 8

            elif column_serial_type in (8, 9):
                column_values.append(int(column_serial_type == 9))

            #blob
            elif column_serial_type >= 12 and column_serial_type % 2 == 0:
                value_len = (column_serial_type - 12) // 2
                column_values.append(buffer[offset : offset + value_len])
                offset += value_len

            #text
            elif column_serial_type >= 13 and column_serial_type % 2 == 1:
                value_len = (column_serial_type - 13) // 2
                column_values.append(buffer[offset : offset + value_len].decode(text_encoding))
                offset += value_len

            else:
                raise NotImplementedError(column_serial_type)

        return column_values, offset - initial_offset



             





            