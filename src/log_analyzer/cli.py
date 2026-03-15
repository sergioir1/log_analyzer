import argparse
from pathlib import Path

from src.models.response_schema import ResponseSchema
from src.core.logger import logger


import argparse
from pathlib import Path

from src.models.response_schema import ResponseSchema
from src.core.logger import logger
from src.utils.date_time import DateTime
from src.utils.error_handler import LogAnalyzerError

SUPPORTED_OPERATIONS: list[str] = ["mfip", "lfip", "eps", "bytes"]

def parse_args():
    parser = argparse.ArgumentParser(
        description="CLI tool to analyze log files"
    )

    # arguments
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Path to one or more input log files"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSON file"
    )

    # options
    parser.add_argument("--mfip", action="store_true", help="Most frequent IP")
    parser.add_argument("--lfip", action="store_true", help="Least frequent IP")
    parser.add_argument("--eps", action="store_true", help="Events per second")
    parser.add_argument("--bytes", action="store_true", help="Total bytes exchanged")

    return parser.parse_args()

def validate_operations(args):
    selected_ops = [op for op in SUPPORTED_OPERATIONS if getattr(args, op)]

    if not selected_ops:
        raise LogAnalyzerError(400, "No operation selected. Please choose at least one valid operation.")

    return selected_ops


def write_output(output_path, response: ResponseSchema, exit_code: int):
    try:
        with open(output_path, "w") as f:
            f.write(response.model_dump_json(indent=4))
        return exit_code
    except Exception as e:
        logger.error(f"CLI: Error while writting output: {e}")
        return 1


def main():
    date_time = DateTime()
    date_time.start()
    exit_code = 0
    args = parse_args()

    from src.log_analyzer.runner_pipeline import RunnerPipeline

    logger.debug("Input files:", args.input)
    logger.debug("Output file:", args.output)

    result = {
        "message": "analysis not implemented yet"
    }
    operations = []
    try:
        operations = validate_operations(args)

        logger.info(f"Operations to execute: {("; ").join(operations)}")

        # aquí más adelante llamaremos al analyzer
        response = RunnerPipeline.run_2(args.input, operations, date_time)
        # analyze_logs(...)

    except LogAnalyzerError as lger:
        date_time.finish()
        logger.error(f"CLI: Error: {lger}")
        response = ResponseSchema(
            status_code=lger.code,
            message=lger.message,
            processing_time=date_time.get_time_diference(),
            operations=operations,
            result={}
        )
        exit_code = 1
    except Exception as e:
        date_time.finish()
        logger.error(f"CLI: Error: {e}")
        response = ResponseSchema(
            status_code=500,
            message=f"Internal server error: {e}",
            processing_time=date_time.get_time_diference(),
            operations=operations,
            result={}
        )
        exit_code = 1

    finally:
        output_path = Path(args.output)
        exit_code = write_output(output_path, response, exit_code)
        logger.info(f"CLI: Results written to {output_path}")
        return exit_code


if __name__ == "__main__":
    import sys
    sys.exit(main())
