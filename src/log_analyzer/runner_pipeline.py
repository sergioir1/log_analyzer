from src.models.response_schema import ResponseSchema
from src.core.logger import logger
from src.operations.analyzer_complete import AnalyzerComplete
from src.utils.date_time import DateTime
from src.utils.error_handler import LogAnalyzerError
from src.utils.parser import select_loader


class RunnerPipeline(object):

    @staticmethod
    def run(file_paths: list[str], operations: list[str], date_time: DateTime, use_spark: bool) -> ResponseSchema:
        # TODO: Build a logic to decide wether to use spark or not (this could be calculating files size)
        try:
            use_spark = use_spark
            logs_entry = select_loader(file_paths, use_spark)

            fun = AnalyzerComplete(loaded_data=logs_entry, operations=operations, use_spark=use_spark)
            fun.process()
            result = fun.generate_result()
            date_time.finish()
            logger.info(f"RUNNER PIPELINE: Processing completed successfully in {date_time.get_time_diference()}")
            response = ResponseSchema(
                status_code=200,
                message="Proccesing Successful",
                processing_time=date_time.get_time_diference(),
                operations=operations,
                result=result
            )

            return response

        except LogAnalyzerError:
            raise
        except Exception as e:
            error_msg = f"RUNNER PIPELINE: Error while processing: {e}"
            logger.error(error_msg)
            raise LogAnalyzerError(500, error_msg)
