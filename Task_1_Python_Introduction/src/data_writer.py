import json

from dict2xml import dict2xml


class DataWriter:
    def __init__(self, write_folder):
        self.path_to_write = write_folder

    def write_info(self, file_name, output_format, info):
        path_to_write = self.path_to_write + file_name
        if output_format == "json":
            result = json.dumps(info, indent=4)
        if output_format == "xml":
            result = dict2xml(info, indent="   ")
        with open(path_to_write, "w", encoding="utf-8") as res_file:
            res_file.write(result)
        print(f"Successfully wrote to file {path_to_write}")
