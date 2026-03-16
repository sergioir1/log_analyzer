import pytest
from unittest.mock import MagicMock, patch

from src.operations.analyzer_complete import AnalyzerComplete
from src.utils.error_handler import LogAnalyzerError


class DummyLogEntry:
    def __init__(self, ip, timestamp, header_size=0, response_size=0):
        self.client_ip = ip
        self.timestamp = timestamp
        self.header_size = header_size
        self.response_size = response_size


def create_entries():
    return [
        DummyLogEntry("1.1.1.1", 1, 10, 20),
        DummyLogEntry("1.1.1.1", 1, 5, 5),
        DummyLogEntry("2.2.2.2", 2, 1, 1),
    ]


# -------------------------
# STREAMING TESTS
# -------------------------

def test_mfip_streaming():
    analyzer = AnalyzerComplete(create_entries(), ["mfip"], use_spark=False)

    analyzer.process()

    result = analyzer.result["MFIP"]
    assert result["value"] == "1.1.1.1"
    assert result["count"] == 2


def test_lfip_streaming():
    analyzer = AnalyzerComplete(create_entries(), ["lfip"], use_spark=False)

    analyzer.process()

    result = analyzer.result["LFIP"]
    assert result["value"] == "2.2.2.2"
    assert result["count"] == 1


def test_eps_streaming():
    analyzer = AnalyzerComplete(create_entries(), ["eps"], use_spark=False)

    analyzer.process()

    result = analyzer.result["EPS"]
    assert result["max"] >= result["min"]
    assert result["mean"] > 0


def test_bytes_streaming():
    analyzer = AnalyzerComplete(create_entries(), ["bytes"], use_spark=False)

    analyzer.process()

    result = analyzer.result["BYTES"]
    assert result["count"] == (10+20+5+5+1+1)


def test_multiple_operations_streaming():
    analyzer = AnalyzerComplete(create_entries(), ["mfip", "bytes"], use_spark=False)

    analyzer.process()

    assert "MFIP" in analyzer.result
    assert "BYTES" in analyzer.result


# -------------------------
# ERROR HANDLING
# -------------------------

def test_process_raises_loganalyzererror():
    analyzer = AnalyzerComplete(create_entries(), ["mfip"], use_spark=False)

    with patch.object(analyzer, "run_streaming", side_effect=Exception("boom")):
        with pytest.raises(LogAnalyzerError):
            analyzer.process()


# -------------------------
# SPARK TESTS (MOCKED)
# -------------------------

@patch("src.operations.analyzer_complete.col")
def test_run_spark_calls_operations(mockmock_col):
    mock_df = MagicMock()

    mock_df.withColumn.return_value = mock_df

    with patch.object(AnalyzerComplete, "run_bytes_spark") as mock_bytes:
        analyzer = AnalyzerComplete(mock_df, ["bytes"], use_spark=True)
        analyzer.df = mock_df

        analyzer.run_spark()
        mock_bytes.return_value = "1111"
        mock_bytes.assert_called_once()

