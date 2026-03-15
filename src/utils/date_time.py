from datetime import datetime, timezone
from src.core.logger import logger

class DateTime(object):

    def __init__(self):
        self.__time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        self.time_start = None
        self.time_end = None

    def get_time_start(self):
        return self.time_start

    def get_time_end(self):
        return self.time_end

    def start(self):
        self.time_start = datetime.now(timezone.utc).strftime(self.__time_format)
        logger.debug(f"Start process time: {self.time_start}")

    def finish(self):
        self.time_end = datetime.now(timezone.utc).strftime(self.__time_format)
        logger.debug(f"Finish process time: {self.time_end}")

    def calculate_time(self):
        if self.time_start is None or self.time_end is None:
            return "00:00:00"
        time1 = datetime.strptime(self.time_start, self.__time_format)
        time2 = datetime.strptime(self.time_end, self.__time_format)
        return time2 - time1


    def get_time_diference(self) -> str:
        duration = self.calculate_time()
        return str(duration)