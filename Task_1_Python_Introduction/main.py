import configparser

from src.arg_parser import ArgParserManager
from src.data_writer import DataWriter
from src.db_manager import DatabaseManager, DataLoader, DataSelector, SchemaManager


def get_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config["MySql"], config["FilePath"]["write_folder"]


def init():
    config_mysql, write_folder = get_config()
    db_manager = DatabaseManager(config_mysql)
    return (
        ArgParserManager(),
        db_manager,
        SchemaManager(db_manager),
        DataLoader(db_manager),
        DataSelector(db_manager),
        DataWriter(write_folder),
    )


def main():
    menu = """
Queries:
        1 - The list of rooms and the number of students in each of them
        2 - 5 rooms with the smallest average age of students
        3- 5 rooms with the biggest difference in the age of students
        4 - List of rooms where students of different sexes live

Choose a query: 1, 2, 3 or 4 or print 0 to exit\n"""
    (
        arg_parser_manager,
        db_manager,
        schema_manager,
        data_loader,
        data_selector,
        data_writer,
    ) = init()
    args = arg_parser_manager.get_arguments()
    if args:
        file_paths = (args.r, args.s)
        output_format = args.f
    else:
        print("Not valid input arguments, check log file to get more information")
        return 0
    schema_manager.create()
    for file_path in file_paths:
        data_loader.load_data_from_file(file_path)
    while True:
        choose = input(menu)
        if choose == "0":
            break
        elif choose not in ("1", "2", "3", "4"):
            print("Please input number from 0 to 4")
        else:
            q_num = int(choose) - 1
            info = data_selector.select_info(q_num)
            file_name = f"{output_format}//query{choose}.{output_format}"
            data_writer.write_info(file_name, output_format, info)
    db_manager.close_conn()
    return 1


if __name__ == "__main__":
    main()
