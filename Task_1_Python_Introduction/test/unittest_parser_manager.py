import unittest
from unittest.mock import patch

from src.arg_parser import ArgParserManager


class ArgumentParserManager(unittest.TestCase):
    @patch(
        "sys.argv",
        [
            "program",
            "-r",
            "json//rooms.json",
            "-s",
            "json//students.json",
            "-f",
            "xml",
        ],
    )
    def test_parse_arguments(self):
        parser = ArgParserManager()
        args = parser.get_arguments()
        self.assertEqual(args.r, "json//rooms.json")
        self.assertEqual(args.s, "json//students.json")
        self.assertEqual(args.f, "xml")

    @patch("sys.argv", ["program"])
    def test_parse_arguments_no_arguments(self):
        parser = ArgParserManager()
        args = parser.get_arguments()
        self.assertIsNone(args)

    (
        "sys.argv",
        [
            "program",
            "-r",
            "json//rooms.json",
            "-s",
            "json//students.json",
            "-f",
            "xmml",
        ],
    )

    def test_parse_arguments_not_valid_arguments(self):
        parser = ArgParserManager()
        is_valid = parser.validate_arguments()
        self.assertEqual(is_valid, False)


if __name__ == "__main__":
    unittest.main()
