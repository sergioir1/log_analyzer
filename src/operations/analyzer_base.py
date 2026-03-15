from abc import ABC, abstractmethod
from typing import Union, Iterator, Any
from pyspark.sql import DataFrame

from src.models.log_entry import LogEntry
from src.utils.date_time import DateTime

class AnalyzerBase(ABC):

    result_analyzer_name: str = None

    def __init__(self,
                 loaded_data: Union[DataFrame | Iterator[LogEntry]],
                 operations: list[str],
                 use_spark: bool = False
                 ):
        self.operations = operations
        self.use_spark = use_spark
        self.entries = loaded_data if not self.use_spark else None
        self.result = {}
        self.df = loaded_data if self.use_spark else None
        self.date_time = DateTime()

    @abstractmethod
    def process(self):
        """Executes the main logic of the operation"""
        pass

    def generate_result(self) -> dict[str, Any]:
        """Returns the result in a uniform dictionary format"""
        self.result["data_process_time"] = self.date_time.get_time_diference()
        return self.result