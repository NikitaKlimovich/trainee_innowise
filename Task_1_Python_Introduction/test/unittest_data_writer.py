import json
import unittest
from unittest.mock import mock_open, patch

from src.data_writer import DataWriter


class TestDataWriter(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open)
    def test_write_info(self, mock_file):
        path_to_write = "res//json//"
        file_name = "query1.html"
        output_format = "json"
        data = [{"id": 1, "name": "Nikita"}, {"id": 2, "name": "Alina"}]
        writer = DataWriter(path_to_write)
        writer.write_info(file_name, output_format, data)

        mock_file.assert_called_once_with(path_to_write + file_name, "w", encoding="utf-8")
        result = json.dumps(data, indent=4)
        mock_file().write.assert_called_once_with(result)


if __name__ == "__main__":
    unittest.main()
