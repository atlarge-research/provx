from typing import Dict, Tuple, List
from schema import CheckpointSize, Size, OutputSize, FullOutputSize, Result, FullResult
from config import results_dir
from pathlib import Path
import re
import json


def load_results(archive_dir: Path) -> Dict[Tuple[str, str], FullResult]:
    input_files = sorted(archive_dir.glob("experiment-*/run-*/lineage-true/metrics.json"))
    results = {}
    for f in input_files:
        withLineage = Result(**json.loads(f.read_text()))
        woLineagePath = Path(str(f).replace("lineage-true", "lineage-false"))
        withoutLineage = Result(**json.loads(woLineagePath.read_text()))
        key = (withoutLineage.algorithm, withLineage.graph)
        if key not in results:
            results[key] = [{"withLineage": withLineage, "withoutLineage": withoutLineage}]
        else:
            results[key].append({"withLineage": withLineage, "withoutLineage": withoutLineage})

    final_results = {}
    for key, value in results.items():
        final_results[key] = FullResult(
            withLineage=[v["withLineage"] for v in value],
            withoutLineage=[v["withoutLineage"] for v in value]
        )
    return final_results


def parse_checkpoint_sizes(archive_dir: Path) -> List[CheckpointSize]:
    checkpoint_size_file = archive_dir / "checkpoint_sizes.txt"
    lines = checkpoint_size_file.read_text().strip().split('\n')
    result = {}

    for line in lines:
        size, _, path = re.split(r"\s+", line)
        lineageId = path.split("/")[-2]
        if not path.endswith(".v"):
            continue
        iteration = int(path.split("/")[-1].replace(".v", ""))
        if lineageId not in result:
            result[lineageId] = []
        result[lineageId].append(Size(iteration=iteration, size=int(size)))

    return [CheckpointSize(lineageId=lineageId, iterations=iters) for lineageId, iters in result.items()]


def parse_output_sizes(archive_dir: Path) -> List[FullOutputSize]:
    output_size_file = archive_dir / "output_sizes.txt"
    lines = output_size_file.read_text().strip().split('\n')
    result = {}

    for line in lines:
        size, _, path = re.split(r"\s+", line)
        stem = path.split("/")[-1]
        algorithm = stem[:stem.find("-")]
        dataset = stem[stem.find("-") + 1:stem.find(".")]
        lineage = True if dataset.endswith("-lineage") else False
        dataset = dataset[:-len("-lineage")] if dataset.endswith("-lineage") else dataset
        if (algorithm, dataset) not in result:
            result[(algorithm, dataset)] = []

        result[(algorithm, dataset)].append(OutputSize(
            algorithm=algorithm,
            graph=dataset,
            lineage=lineage,
            size=int(size)
        ))

    sizes = []
    for (algorithm, dataset), v in result.items():
        sizes.append(FullOutputSize(
            withLineage=v[0] if v[0].lineage else v[1],
            withoutLineage=v[0] if not v[0].lineage else v[1]
        ))

    return sizes
