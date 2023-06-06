from typing import Dict, Tuple, List

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from config import plots_dir, results_dir
from schema import CheckpointSize, FullResult
from utils import pretty_size
import ingest 
import plots


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
    for archive_dir in results_dir.iterdir():
        if archive_dir.is_file():
            continue

        results = ingest.load_results(archive_dir)
        output_sizes = ingest.parse_output_sizes(archive_dir)
        checkpoint_sizes = ingest.parse_checkpoint_sizes(archive_dir)

        p_dir = plots_dir / archive_dir.name
        if not p_dir.exists():
            p_dir.mkdir()

        plots.execution_overhead(results, p_dir)
        plots.execution_overhead(results, p_dir, single=True)
        plots.iterations_per_dataset(results, p_dir)

        plots.storage_overhead(results, output_sizes, checkpoint_sizes, p_dir)

        #plots.messages_per_iteration(results)

        # graph_size_per_iteration(results, checkpoint_sizes)

if __name__ == '__main__':
    main()
