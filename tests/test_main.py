import builtins
from unittest.mock import patch, MagicMock
import pytest
from pathlib import Path

from src.main import main, validate_operations, SUPPORTED_OPERATIONS
from src.utils.error_handler import LogAnalyzerError
from src.models.response_schema import ResponseSchema



# Tests validate_operations
class ArgsMock:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_validate_operations_success():
    args = ArgsMock(mfip=True, lfip=False, eps=False, bytes=False)
    ops = validate_operations(args)
    assert ops == ["mfip"]


def test_validate_operations_no_ops():
    args = ArgsMock(mfip=False, lfip=False, eps=False, bytes=False)
    with pytest.raises(LogAnalyzerError) as exc:
        validate_operations(args)
    assert exc.value.code == 400



# Tests main CLI
@patch("src.main.RunnerPipeline")
@patch("builtins.open", new_callable=MagicMock)
@patch("src.main.parse_args")
def test_main_success(mock_parse_args, mock_open, mock_runner):
    # Configurar mocks
    mock_parse_args.return_value = ArgsMock(
        input=["file.log"], output="out.json", mfip=True, lfip=False, eps=False, bytes=False, use_spark=False
    )
    mock_runner.run.return_value = ResponseSchema(
        status_code=200, message="ok", processing_time="1.0", operations=["mfip"], result={"data": 123}
    )

    exit_code = main()
    assert exit_code == 0
    mock_open.assert_called_once_with(Path("out.json"), "w")
    mock_runner.run.assert_called_once()


@patch("src.main.RunnerPipeline")
@patch("builtins.open", new_callable=MagicMock)
@patch("src.main.parse_args")
def test_main_loganalyzer_error(mock_parse_args, mock_open, mock_runner):
    mock_parse_args.return_value = ArgsMock(
        input=["file.log"], output="out.json", mfip=False, lfip=False, eps=False, bytes=False, use_spark=False
    )
    exit_code = main()
    assert exit_code == 1
    mock_open.assert_called_once()  # el archivo aun se intenta escribir


@patch("src.main.RunnerPipeline")
@patch("builtins.open", new_callable=MagicMock)
@patch("src.main.parse_args")
def test_main_exception(mock_parse_args, mock_open, mock_runner):
    mock_parse_args.return_value = ArgsMock(
        input=["file.log"], output="out.json", mfip=True, lfip=False, eps=False, bytes=False, use_spark=False
    )
    mock_runner.run.side_effect = Exception("Boom")
    exit_code = main()
    assert exit_code == 1


@patch("src.main.RunnerPipeline")
@patch("builtins.open", new_callable=MagicMock)
@patch("src.main.parse_args")
def test_main_write_output_fail(mock_parse_args, mock_open, mock_runner):
    mock_parse_args.return_value = ArgsMock(
        input=["file.log"], output="out.json", mfip=True, lfip=False, eps=False, bytes=False, use_spark=False
    )
    mock_runner.run.return_value = ResponseSchema(
        status_code=200, message="ok", processing_time="1.0", operations=["mfip"], result={"data": 123}
    )
    mock_open.side_effect = IOError("Disk full")
    exit_code = main()
    assert exit_code == 1