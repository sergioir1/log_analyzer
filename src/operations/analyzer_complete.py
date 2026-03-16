from collections import Counter
from typing import Union, Iterator

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.models.log_entry import LogEntry
from src.operations.analyzer_base import AnalyzerBase
from src.core.logger import logger
from src.utils.error_handler import LogAnalyzerError

class AnalyzerComplete(AnalyzerBase):

    def __init__(self,
                 loaded_data: Union[DataFrame | Iterator[LogEntry]],
                 operations: list[str],
                 use_spark: bool = False
                 ):
        super().__init__(loaded_data, operations, use_spark)
        self.streaming_function_by_operation = {
            "mfip": self.run_mfip_streaming,
            "lfip": self.run_lfip_streaming,
            "eps": self.run_eps_streaming,
            "bytes": self.run_bytes_streaming
        }

        self.streaming_operations_data = {
            "mfip": Counter(),
            "lfip": Counter(),
            "eps": Counter(),
            "bytes": 0
        }

        self.streaming_result_functions = {
            "mfip": self.run_mfip_streaming_result,
            "lfip": self.run_lfip_streaming_result,
            "eps": self.run_eps_streaming_result,
            "bytes": self.run_bytes_streaming_result
        }

        self.spark_function_by_operation = {
            "mfip": self.run_mfip_lfip_spark,
            "lfip": self.run_mfip_lfip_spark,
            "eps": self.run_eps_spark,
            "bytes": self.run_bytes_spark
        }


    def process(self):
        try:
            logger.info(f"ANALYZER: Start of the operations process")
            self.date_time.start()
            if self.use_spark:
                self.run_spark()
            else:
                self.run_streaming()

            self.date_time.finish()
        except Exception as e:
            error_msg = f"ANALYZER: Error while processing operations: {e}"
            logger.error(error_msg)
            raise LogAnalyzerError(500, error_msg)


    def run_streaming(self):
        logger.debug("ANALYZER MFIP: Running streaming process")

        for entry in self.entries:
            self.run_streaming_entries_by_operations(entry)

        for op in self.operations:
            self.result_analyzer_name = op.upper()
            self.streaming_result_functions[op]()


    def run_streaming_entries_by_operations(self, entry: LogEntry):
        for op in self.operations:
            self.streaming_function_by_operation[op](entry)

    def run_mfip_streaming(self, entry: LogEntry):
        self.streaming_operations_data["mfip"][entry.client_ip] += 1

    def run_mfip_streaming_result(self):
        counter_mfip = self.streaming_operations_data["mfip"]
        if counter_mfip:
            ip, counter = counter_mfip.most_common(1)[0]
            logger.info(f"ANALYZER MFIP: Most frecuent IP: {(ip, counter)}")
            self.result[self.result_analyzer_name] = {
                "value": ip,
                "count": counter
            }
        else:
            self.result[self.result_analyzer_name] = {}

    def run_lfip_streaming(self, entry: LogEntry):
        self.streaming_operations_data["lfip"][entry.client_ip] += 1

    def run_lfip_streaming_result(self):
        counter_lfip = self.streaming_operations_data["lfip"]
        if counter_lfip:
            ip, counter = counter_lfip.most_common()[-1]
            logger.info(f"ANALYZER LFIP: Last frecuent IP: {(ip, counter)}")
            self.result[self.result_analyzer_name] = {
                "value": ip,
                "count": counter
            }
        else:
            self.result[self.result_analyzer_name] = {}

    def run_eps_streaming(self, entry: LogEntry):
        second = int(entry.timestamp)  # Trunca a segundo entero
        self.streaming_operations_data["eps"][second] += 1

    def run_eps_streaming_result(self):
        counter_eps = self.streaming_operations_data["eps"]
        total_events = sum(counter_eps.values())
        total_seconds = len(counter_eps)

        eps_mean = round(total_events / total_seconds,2) if total_seconds > 0 else 0
        eps_max = max(counter_eps.values()) if counter_eps else 0
        eps_min = min(counter_eps.values()) if counter_eps else 0

        logger.info("ANALYZER EPS: Events per second - Max: {}, Mean: {}, Min: {}".format(
            eps_max, eps_mean, eps_min)
        )

        self.result[self.result_analyzer_name] = {
            "max": eps_max,
            "mean":eps_mean,
            "min": eps_min
        }

    def run_bytes_streaming(self, entry: LogEntry):
        self.streaming_operations_data["bytes"] += entry.header_size + entry.response_size

    def run_bytes_streaming_result(self):
        logger.info(f"ANALYZER BYTES: Total amount of bytes exchanged {self.streaming_operations_data["bytes"]}")
        self.result[self.result_analyzer_name] = {
            "count": self.streaming_operations_data["bytes"]
        }

    def run_spark(self):
        self.df = (
            self.df
            .withColumn("second", col("timestamp").cast("long"))
            .withColumn("bytes", col("response_size") + col("header_size"))
            # .persist(StorageLevel.MEMORY_AND_DISK)
        )

        if "mfip" in self.operations or "lfip" in self.operations:
            self.run_mfip_lfip_spark(
                mfip="mfip" in self.operations,
                lfip="lfip" in self.operations
            )
        operations = [op for op in self.operations if op not in ("mfip", "lfip")]

        for op in operations:
            self.result_analyzer_name = op.upper()
            self.spark_function_by_operation[op]()

    def run_mfip_lfip_spark(self, mfip: bool, lfip: bool):
        logger.info("ANALYZER MFIP-LFIP: Running MFIP-LFIP operation with SPARK process")
        frecuent_ip = (self.df.groupBy("client_ip").count().persist(StorageLevel.MEMORY_AND_DISK))
                       # .cache())

        if mfip:
            mfip_result = frecuent_ip.orderBy(col("count").desc()).limit(1).collect()
            if mfip_result:
                row = mfip_result[0]
                logger.info(f"ANALYZER MFIP: Most frecuent IP: {row}")
                self.result["MFIP"] = {
                    "value": row["client_ip"],
                    "count": row["count"]
                }
            else:
                logger.warning(f"ANALYZER MFIP: No ips found in query")
                self.result["MFIP"] = {}

        if lfip:
            lfip_result = frecuent_ip.orderBy(col("count").asc()).limit(1).collect()
            if lfip_result:
                row = lfip_result[0]
                logger.info(f"ANALYZER LFIP: Least frecuent IP: {row}")

                self.result["LFIP"] = {
                    "value": row["client_ip"],
                    "count": row["count"]
                }
            else:
                logger.warning(f"ANALYZER LFIP: No ips found in query")
                self.result["LFIP"] = {}

    def run_eps_spark(self):
        from pyspark.sql.functions import count, avg, max, min
        logger.info("ANALYZER EPS: Running EPS operation with SPARK process")

        eps_row = (
            self.df.groupBy("second")
            .agg(count("*").alias("events"))
            .agg(
                avg("events").alias("avg_eps"),
                max("events").alias("max_eps"),
                min("events").alias("min_eps")
            )
            .collect()[0]
        )

        eps_max = eps_row["max_eps"]
        eps_mean = eps_row["avg_eps"]
        eps_min = eps_row["min_eps"]
        logger.info("ANALYZER EPS: Events per second - Max: {}, Mean: {}, Min: {}".format(
            eps_max, eps_mean, eps_min)
        )

        self.result[self.result_analyzer_name] = {
            "max": eps_max,
            "mean": eps_mean,
            "min": eps_min
        }

    def run_bytes_spark(self):
        from pyspark.sql.functions import sum
        logger.info("ANALYZER BYTES: Running BYTES operation with SPARK process")

        row = self.df.agg(
            sum("bytes").alias("total_bytes")
        ).collect()[0]

        bytes_sum = row["total_bytes"]
        logger.info(f"ANALYZER BYTES: Total amount of bytes exchanged {bytes_sum}")

        self.result[self.result_analyzer_name] = {
            "count": bytes_sum
        }