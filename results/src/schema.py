from typing import List
from pydantic import BaseModel

class Parameters(BaseModel):
    id: str | None
    experimentID: str | None
    dataset: str
    algorithm: str
    runNr: int
    outputDir: str
    configPath: str | None
    graphalyticsConfigPath: str | None
    lineageDir: str
    setup: str
    tracingEnabled: bool
    storageEnabled: bool
    storageFormat: str | None
    # compressionEnabled: bool
    applicationId: str
    executorCount: int
    storageFormat: str


class Inputs(BaseModel):
    config: str
    vertices: str
    edges: str
    parameters: Parameters


class Node(BaseModel):
    id: int
    location: str


class Value1(BaseModel):
    amount: str
    unit: str


class Value(BaseModel):
    type: str
    name: str
    value: Value1


class Metrics(BaseModel):
    type: str
    values: List[List[Value]]


class Edge(BaseModel):
    source: int
    target: int
    relationship: str
    type: str
    metrics: Metrics


class Graph(BaseModel):
    nodes: List[Node]
    edges: List[Edge]


class IndividualItem(BaseModel):
    graphID: int
    size: int
    location: str


class Sizes(BaseModel):
    total: int
    individual: List[IndividualItem]


class Duration(BaseModel):
    amount: int
    unit: str


class Value2(BaseModel):
    type: str
    name: str
    value: str


class Metrics1(BaseModel):
    type: str
    values: List[List[Value2]]


class Metric(BaseModel):
    graphID: int
    metrics: Metrics1


class Outputs(BaseModel):
    stdout: str
    stderr: str
    results: str
    graph: Graph
    sizes: Sizes
    duration: Duration
    # metrics: List[Metric]
    # lineageDirectory: str


class Model(BaseModel):
    inputs: Inputs
    outputs: Outputs
