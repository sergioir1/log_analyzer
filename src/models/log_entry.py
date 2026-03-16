from dataclasses import dataclass
from typing import Optional


@dataclass
class LogEntry(object):
    timestamp: float
    response_size: int
    client_ip: str
    header_size: Optional[int] = None
    response_code: Optional[str] = None
    method: Optional[str] = None
    url:Optional[ str] = None
    user: Optional[str] = None
    destination_ip: Optional[str] = None
    response_type: Optional[str] = None