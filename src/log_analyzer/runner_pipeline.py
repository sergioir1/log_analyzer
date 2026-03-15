from src.models.response_schema import ResponseSchema
from src.core.logger import logger
from src.operations.analyzer_complete import AnalyzerComplete
from src.operations.analyzer_lfip import AnalyzerLFIP
from src.operations.analyzer_mfip import AnalyzerMFIP
from src.utils.date_time import DateTime
from src.utils.parser import select_loader


class RunnerPipeline(object):

    analyzer_functions = {
        "mfip": AnalyzerMFIP,
        "lfip": AnalyzerLFIP
    }

    @staticmethod
    def run(file_paths: list[str], operations: list[str]) -> ResponseSchema:
        # TODO: Build a logic to decide wether to use spark or not (this could be calculating files size)
        use_spark = True
        logs_entry = select_loader(file_paths, use_spark)

        results = []
        for op in operations:
            logger.info(f"RUNNER PIPELINE : Runnig pipeline for operation {op}")
            fun = RunnerPipeline.analyzer_functions[op](loaded_data=logs_entry, use_spark=use_spark)
            fun.process()
            results.append(fun.generate_result())

        result = {k: v for d in results for k, v in d.items()}
        response = ResponseSchema(
            status_code=200,
            message="Proccesing Successful",
            operations=operations,
            result=result
        )

        return response

    @staticmethod
    def run_2(file_paths: list[str], operations: list[str], date_time: DateTime) -> ResponseSchema:
        # TODO: Build a logic to decide wether to use spark or not (this could be calculating files size)
        use_spark = True
        logs_entry = select_loader(file_paths, use_spark)

        fun = AnalyzerComplete(loaded_data=logs_entry, operations=operations, use_spark=use_spark)
        fun.process()
        result = fun.generate_result()
        # result = {k: v for d in results for k, v in d.items()}
        date_time.finish()
        response = ResponseSchema(
            status_code=200,
            message="Proccesing Successful",
            processing_time=date_time.get_time_diference(),
            operations=operations,
            result=result
        )

        return response

