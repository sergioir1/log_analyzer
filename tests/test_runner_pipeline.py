import pytest
from unittest.mock import MagicMock, patch

from src.log_analyzer.runner_pipeline import RunnerPipeline
from src.utils.error_handler import LogAnalyzerError
from src.models.response_schema import ResponseSchema


@patch("src.log_analyzer.runner_pipeline.select_loader")
@patch("src.log_analyzer.runner_pipeline.AnalyzerComplete")
def test_runner_pipeline_success(mock_analyzer, mock_loader):
    file_paths = ["log1.log"]
    operations = ["mfip"]
    use_spark = False

    mock_date = MagicMock()
    mock_date.get_time_diference.return_value = "1.00"

    mock_loader.return_value = "logs"

    analyzer_instance = MagicMock()
    analyzer_instance.generate_result.return_value = {"mfip": "127.0.0.1"}
    mock_analyzer.return_value = analyzer_instance

    response = RunnerPipeline.run(file_paths, operations, mock_date, use_spark)

    assert isinstance(response, ResponseSchema)
    assert response.status_code == 200
    assert response.result == {"mfip": "127.0.0.1"}

    mock_loader.assert_called_once_with(file_paths, use_spark)
    analyzer_instance.process.assert_called_once()
    analyzer_instance.generate_result.assert_called_once()
    mock_date.finish.assert_called_once()


@patch("src.log_analyzer.runner_pipeline.select_loader")
def test_runner_pipeline_rethrow_loganalyzer_error(mock_loader):
    file_paths = ["log1.log"]
    operations = ["mfip"]
    use_spark = False

    mock_date = MagicMock()

    mock_loader.side_effect = LogAnalyzerError(400, "test error")

    with pytest.raises(LogAnalyzerError):
        RunnerPipeline.run(file_paths, operations, mock_date, use_spark)


@patch("src.log_analyzer.runner_pipeline.select_loader")
@patch("src.log_analyzer.runner_pipeline.logger")
def test_runner_pipeline_wrap_generic_exception(mock_logger, mock_loader):
    file_paths = ["log1.log"]
    operations = ["mfip"]
    use_spark = False

    mock_date = MagicMock()

    mock_loader.side_effect = Exception("unexpected error")

    with pytest.raises(LogAnalyzerError) as exc:
        RunnerPipeline.run(file_paths, operations, mock_date, use_spark)

    assert exc.value.code == 500
    assert "RUNNER PIPELINE: Error while processing" in exc.value.message
    mock_logger.error.assert_called_once()