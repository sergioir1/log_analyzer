import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.parser import (
    StreamingLoader,
    SparkLogLoader,
    validate_paths,
    select_loader,
)


# -----------------------
# StreamingLoader.parse_line
# -----------------------

def test_parse_line_valid():
    loader = StreamingLoader()

    line = "1.0 10 192.168.0.1 200 20 GET /index user1 10.0.0.1 text/html"
    entry = loader.parse_line(line)

    assert entry.timestamp == 1.0
    assert entry.header_size == 10
    assert entry.client_ip == "192.168.0.1"
    assert entry.response_size == 20


def test_parse_line_malformed():
    loader = StreamingLoader()

    line = "bad line"
    entry = loader.parse_line(line)

    assert entry is None


def test_parse_line_value_error():
    loader = StreamingLoader()

    line = "abc 10 ip 200 20 GET /index user1 10.0.0.1 text/html"
    entry = loader.parse_line(line)

    assert entry is None


# -----------------------
# parse_file
# -----------------------

def test_parse_file_normal(tmp_path):

    file = tmp_path / "log.txt"

    file.write_text(
        "1 10 ip1 200 20 GET / user ip2 text\n"
        "2 10 ip2 200 30 GET / user ip3 text\n"
    )

    loader = StreamingLoader()

    results = list(loader.parse_file(file))

    assert len(results) == 2


def test_parse_file_gzip(tmp_path):

    file = tmp_path / "log.gz"

    with gzip.open(file, "wt") as f:
        f.write("1 10 ip1 200 20 GET / user ip2 text\n")

    loader = StreamingLoader()

    results = list(loader.parse_file(file))

    assert len(results) == 1


# -----------------------
# load multiple files
# -----------------------

def test_load_multiple_files(tmp_path):

    f1 = tmp_path / "a.log"
    f2 = tmp_path / "b.log"

    f1.write_text("1 10 ip1 200 20 GET / user ip2 text\n")
    f2.write_text("2 10 ip2 200 30 GET / user ip3 text\n")

    loader = StreamingLoader()

    results = list(loader.load([str(f1), str(f2)]))

    assert len(results) == 2


# -----------------------
# validate_paths
# -----------------------

def test_validate_paths_ok(tmp_path):

    f = tmp_path / "file.log"
    f.write_text("test")

    paths = validate_paths([str(f)])

    assert paths == [str(f)]


def test_validate_paths_file_not_found():

    with pytest.raises(FileNotFoundError):
        validate_paths(["not_exists.log"])


# -----------------------
# select_loader
# -----------------------

def test_select_loader_streaming(tmp_path):

    f = tmp_path / "file.log"
    f.write_text("1 10 ip1 200 20 GET / user ip2 text\n")

    with patch("src.utils.parser.StreamingLoader") as mock_loader:

        instance = MagicMock()
        instance.load.return_value = iter([])
        mock_loader.return_value = instance

        select_loader([str(f)], use_spark=False)

        mock_loader.assert_called_once()


def test_select_loader_spark(tmp_path):

    f = tmp_path / "file.log"
    f.write_text("1 10 ip1 200 20 GET / user ip2 text\n")

    with patch("src.utils.parser.SparkLogLoader") as mock_loader:

        instance = MagicMock()
        instance.load.return_value = MagicMock()
        mock_loader.return_value = instance

        select_loader([str(f)], use_spark=True)

        mock_loader.assert_called_once()


# -----------------------
# SparkLogLoader
# -----------------------

@patch("src.utils.parser.SparkSession")
def test_spark_loader_creation(mock_spark):

    builder = MagicMock()
    mock_spark.builder = builder

    builder.appName.return_value = builder
    builder.getOrCreate.return_value = MagicMock()

    loader = SparkLogLoader()

    assert loader.spark is not None
