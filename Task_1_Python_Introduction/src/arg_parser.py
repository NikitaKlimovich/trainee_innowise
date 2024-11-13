import argparse
import logging
import os


logging.basicConfig(
    level=logging.INFO,
    filename="log//py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s",
)


class ArgParserManager:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.is_valid = True
        self.parser.add_argument("-r", metavar="rooms_file_path", help="Path to file rooms.json")
        self.parser.add_argument(
            "-s",
            metavar="students_file_path",
            help="Path to file students.json",
        )
        self.parser.add_argument(
            "-f",
            metavar="format",
            help="Choose output file format (json or xml)",
        )
        self.parser = self.parser.parse_args()

    @staticmethod
    def validate_file_path(file_path):
        if not os.path.exists(file_path):
            logging.error(f"File {file_path} doesn't exist")
            return False
        return True

    @staticmethod
    def validate_output_format(output_format):
        if output_format not in ("json", "xml"):
            logging.error(
                "Format {output_format} doesn't supported by program, choose 'json' or 'xml'"
            )
            return False
        return True

    def validate_arguments(self):
        if not (self.parser.r and self.parser.s and self.parser.f):
            logging.error("Please, use all arguments")
            return False
        return (
            ArgParserManager.validate_file_path(self.parser.r)
            * ArgParserManager.validate_file_path(self.parser.s)
            * ArgParserManager.validate_output_format(self.parser.f)
        )

    def get_arguments(self):
        if self.validate_arguments():
            return self.parser
