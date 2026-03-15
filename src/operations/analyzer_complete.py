from collections import Counter
from typing import Union, Iterator

from pyspark.sql import DataFrame

from src.models.log_entry import LogEntry
from src.operations.analyzer_base import AnalyzerBase
from src.core.logger import logger

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
        logger.info(f"ANALYZER MFIP: Starting analyzer mfip process")
        self.date_time.start()
        if self.use_spark:
            self.run_spark()
        else:
            self.run_streaming()

        self.date_time.finish()


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
        frecuent_ip = self.df.groupBy("client_ip").count().orderBy("count", ascending=False)

        if mfip:
            mfip_result = frecuent_ip.first()
            logger.info("ANALYZER MFIP: Most frecuent IP: " + str(mfip_result))
            mfip_ip = mfip_result["client_ip"] if mfip_result else None
            mfip_count = mfip_result["count"] if mfip_result else None

            self.result["MFIP"] = {
                "value": mfip_ip,
                "count": mfip_count
            }
        if lfip:
            lfip_result = frecuent_ip.tail(1)[0]
            logger.info("ANALYZER LFIP: Last frecuent IP: " + str(lfip_result))
            lfip_ip = lfip_result["client_ip"] if lfip_result else None
            lfip_count = lfip_result["count"] if lfip_result else None

            self.result["LFIP"] = {
                "value": lfip_ip,
                "count": lfip_count
            }


    def run_eps_spark(self):
        from pyspark.sql.functions import col, count, avg, max, min
        logger.info("ANALYZER EPS: Running EPS operation with SPARK process")

        events_per_second = (self.df.withColumn("second", col("timestamp").cast("long"))
            .groupBy("second")
            .agg(count("*").alias("events"))
        )
        stats = events_per_second.agg(
            avg("events").alias("avg_eps"),
            max("events").alias("max_eps"),
            min("events").alias("min_eps")
        )
        row = stats.collect()[0]
        eps_max = row["max_eps"]
        eps_mean = row["avg_eps"]
        eps_min = row["min_eps"]
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
        total_bytes = self.df.agg(
            sum("response_size").alias("sum_response_size"),
            sum("header_size").alias("sum_header_size")
        )
        row = total_bytes.collect()[0]
        bytes_sum = row["sum_response_size"] + row["sum_header_size"]
        logger.info(f"ANALYZER BYTES: Total amount of bytes exchanged {bytes_sum}")

        self.result[self.result_analyzer_name] = {
            "count": bytes_sum
        }