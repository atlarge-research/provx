from typing import Dict, Tuple, List
from schema import FullResult
from utils import pretty_size
import ingest 
import plots

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from schema import CheckpointSize, FullOutputSize



def graph_size_per_iteration(results: Dict[Tuple[str, str], FullResult], checkpoints: List[CheckpointSize]):

    for (algorithm, graph), v in results.items():
        if v.withLineage.metadata.lineageDirectory is None:
            continue

        lineageDirectory = v.withLineage.metadata.lineageDirectory
        iterations = [checkpoint for checkpoint in checkpoints if checkpoint.lineageId == lineageDirectory]
        print(iterations)

        print(algorithm, graph, v.withLineage.metadata.lineageDirectory)
    return

    indices = list(range(len(sizes)))
    labels = [str(index) for index in indices]

    fig, ax = plt.subplots()

    ax.bar(indices, sizes)
    ax.set_xlabel("Index")
    ax.set_ylabel("Size")
    ax.set_title(f"Sizes for {dataset_name} using {algorithm_name}")

    # Format y-axis to display human-readable sizes
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: pretty_size(y)))

    for index, size in zip(indices, sizes):
        plt.text(index, size, str(index), ha="center", va="bottom")

    plt.show()



def main():
    results = ingest.load_results()
    output_sizes = ingest.parse_output_sizes()
    checkpoint_sizes = ingest.parse_checkpoint_sizes()

    # breakpoint()

    # plots.messages_per_iteration(results)
    plots.execution_overhead(results)

    # TODO: compare to checkpoint storage size
    plots.storage_overhead(results, output_sizes, checkpoint_sizes)

    # graph_size_per_iteration(results, checkpoint_sizes)

if __name__ == '__main__':
    main()
