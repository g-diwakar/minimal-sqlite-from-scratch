import sys

# from dataclasses import dataclass

from .utils import Sqlite

# import sqlparse - available if you need it!

# database_file_path = sys.argv[1]
# command = sys.argv[2]

# if command == ".dbinfo":
#     with open(database_file_path, "rb") as database_file:
#         # You can use print statements as follows for debugging, they'll be visible when running tests.
#         print("Logs from your program will appear here!")

#         # Uncomment this to pass the first stage
#         database_file.seek(16)  # Skip the first 16 bytes of the header
#         page_size = int.from_bytes(database_file.read(2), byteorder="big")
#         print(f"database page size: {page_size}")
# else:
#     print(f"Invalid command: {command}")

if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2].lower()

    available_command = [".dbinfo",".tables","select"]

    TABLE_NAME = None

    if command.startswith("select"):
        KEYWORDS_LIST = command.split(" ")
        TABLE_NAME = KEYWORDS_LIST[KEYWORDS_LIST.index("from")+1]
        command = "select"

    if command in available_command :
        with open(database_file_path,"rb") as db_file:
            cursor = Sqlite(db_file,TABLE_NAME)
            cursor.run(command)
            

    else:
        print(f"Invalid command: {command}")
