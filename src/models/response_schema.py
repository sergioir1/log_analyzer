from typing import Literal

from pydantic import BaseModel, Field


class ResponseSchema(BaseModel):
    status_code: int = Field(
        description="HTTP status code of the response",
        examples=[200],
        default=200,
    )
    message: str = Field(
        description="A message describing the result of the operation",
        examples=["Operation completed successfully"],
        default="Operation(s) completed successfully",
    )
    processing_time: str = Field(
        description="The processing time of the operation",
        examples=["0:01:00.841924"],
    )
    operations: list[str] = Field(
        description="Operations requested/executed by the analyzer",
        examples=[["mfip", "eps"]],
        default_factory=list,
    )
    result: dict = Field(
        description = "The result of the analysis, structured as a dictionary",
        examples = [{"MFIP": "IP result", "EPS": "EPS result"}],
        default_factory = dict
    )
