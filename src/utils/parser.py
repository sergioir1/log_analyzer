from typing import Iterator, List, Union
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, size

from src.models.log_entry import LogEntry
from src.core.logger import logger

class StreamingLoader(object):

    def parse_line(self, line: str) -> LogEntry | None:
        """
        Parse a single log line into a LogEntry object.
        Returns None if the line cannot be parsed.
        """

        parts = line.strip().split()

        if len(parts) < 10:
            logger.warning(f"PARSER: Malformed line skipped: {line}")
            return None

        try:
            timestamp = float(parts[0])
            header_size = int(parts[1])
            client_ip = parts[2]
            response_code = parts[3]
            response_size = int(parts[4])
            method = parts[5]
            url = parts[6]
            user = parts[7]
            destination_ip = parts[8]
            response_type = parts[9]

            return LogEntry(
                timestamp=timestamp,
                header_size=header_size,
                client_ip=client_ip,
                response_code=response_code,
                response_size=response_size,
                method=method,
                url=url,
                user=user,
                destination_ip=destination_ip,
                response_type=response_type
            )

        except (ValueError, IndexError):
            # TODO: Revisar hacer con este except
            return None


    def parse_file(self, file_path: Path) -> Iterator[LogEntry]:
        """
        Stream parse a log file yielding LogEntry objects.
        """

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                entry = self.parse_line(line)
                if entry:
                    yield entry


    def load(self, file_paths: list[str]) -> Iterator[LogEntry]:
        """
        Parse multiple log files.
        """
        for file_path in file_paths:
            path = Path(file_path)
            yield from self.parse_file(path)


class SparkLogLoader(object):

    def __init__(self, app_name: str = "LogAnalyzer"):
        logger.info("PARSER: Creating or getting Spark Session")
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def load(self, file_paths: List[str]) -> DataFrame:
        """
        Load multiple log files into a Spark DataFrame.
        """

        # read multiple files
        df = self.spark.read.text(file_paths)
        parts = split(col("value"), r"\s+")
        df_parsed = (
            df.withColumn("parts", parts)
            .filter(size(col("parts")) >= 10)
            .select(
                col("parts")[0].cast("double").alias("timestamp"),
                col("parts")[1].cast("int").alias("header_size"),
                col("parts")[2].alias("client_ip"),
                col("parts")[3].alias("response_code"),
                col("parts")[4].cast("int").alias("response_size"),
                col("parts")[5].alias("method"),
                col("parts")[6].alias("url"),
                col("parts")[7].alias("user"),
                col("parts")[8].alias("destination_ip"),
                col("parts")[9].alias("response_type"),
            )
        )
        # TODO: Conteo filas nulas para saber cuantos datos han sido mal cargados

        return df_parsed

def validate_paths(file_paths: List[str]) -> List[str]:

    paths = []

    for file_path in file_paths:
        path = Path(file_path)
        if not path.exists():
            logger.error(f"PARSER: File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        paths.append(str(path))

    return paths

def select_loader(file_paths: List[str], use_spark: bool = False) -> Union[DataFrame | Iterator[LogEntry]]:
    file_paths = validate_paths(file_paths)
    if use_spark:
        logger.info("PARSER: Loading data from Spark")
        loader = SparkLogLoader()
    else:
        logger.info("PARSER: Loading data from Files")
        loader = StreamingLoader()
    return loader.load(file_paths)