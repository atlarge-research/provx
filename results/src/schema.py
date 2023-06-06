from typing import Optional, List
from pydantic import BaseModel

class PregelIteration(BaseModel):
    idx: int
    messageCount: int

class Duration(BaseModel):
    duration: int
    unit: str

class Metadata(BaseModel):
    iterations: Optional[List[PregelIteration]] = None
    lineageDirectory: Optional[str] = None
    duration: Duration

class Result(BaseModel):
    algorithm: str
    graph: str
    lineage: bool
    runNr: int
    metadata: Metadata

class FullResult(BaseModel):
    withLineage: List[Result]
    withoutLineage: List[Result]

class Size(BaseModel):
    iteration: int
    size: int

class CheckpointSize(BaseModel):
    lineageId: str
    iterations: List[Size]

class OutputSize(BaseModel):
    algorithm: str
    graph: str
    lineage: bool
    size: int

class FullOutputSize(BaseModel):
    withLineage: OutputSize
    withoutLineage: OutputSize
