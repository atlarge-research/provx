from typing import Dict, Tuple
from schema import FullResult

import matplotlib.pyplot as plt
from config import plots_dir

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import numpy as np

algorithm_names = {
    "pr": "PageRank",
    "sssp": "Single Source Shortest Paths",
    "bfs": "Breadth-First Search",
    "lcc": "Local Clustering Coefficient",
    "wcc": "Weakly Connected Components",
    "cdlp": "Community Detection using Label Propagation",
}

algorithm_names_short = {
    "pr": "PageRank",
    "sssp": "SSSP",
    "bfs": "BFS",
    "lcc": "LCC",
    "wcc": "WCC",
    "cdlp": "CDLP",
}

def messages_per_iteration(results: Dict[Tuple[str, str], FullResult]):
    for ((algorithm, graph), result) in results.items():
        iterations_data = result.withLineage[0].metadata.iterations

        # TODO: add error bars

        if iterations_data is None:
            print(f"Skipping {algorithm} on {graph} because it has no iterations data")
            continue

        iterations, messages = zip(*[(iter.idx + 1, iter.messageCount) for iter in iterations_data])

        plt.bar(iterations, messages)
        plt.xlabel('Iterations')
        plt.ylabel('Number of Messages')
        plt.title(f'{algorithm_names[algorithm]} on {graph}')
        plt.yscale("log")
        plt.xticks(range(min(iterations), max(iterations) + 1))
        plt.savefig(plots_dir / f"{algorithm}-{graph}.pdf")
        plt.tight_layout()
        plt.clf()


def execution_overhead(results: Dict[Tuple[str, str], FullResult]):
    data = []
    for (algorithm, dataset), r in results.items():
        per_run = []
        for idx, wl in enumerate(r.withLineage):
            wl_duration = wl.metadata.duration.duration
            wol_duration = r.withoutLineage[idx].metadata.duration.duration
            overhead = (wl_duration / wol_duration) * 100 - 100
            per_run.append(overhead)
        mean = np.mean(per_run)
        std = np.std(per_run)
        data.append((algorithm, dataset, mean, std))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    bar_width = 1 / (num_algorithms + 1)

    _, ax = plt.subplots()

    for idx, algorithm in enumerate(algorithms):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for dataset in datasets]
        errors = [next((entry[3] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for dataset in datasets]
        positions = [i + idx * bar_width for i in range(num_datasets)]
        ax.bar(positions, sizes, yerr=errors, width=bar_width, label=algorithm_names_short[algorithm], capsize=2, align='center')

    ax.set_title('Overhead of lineage storage on execution duration')
    ax.set_ylabel('Overhead compared to non-lineage execution')
    ax.set_xlabel("Dataset")
    ax.set_xticks([i + bar_width * (num_algorithms - 1) / 2 for i in range(num_datasets)])
    ax.set_xticklabels(datasets, rotation=45, ha='right')
    ax.yaxis.set_major_formatter(ticker.PercentFormatter())
    ax.legend()
    plt.savefig(plots_dir / "overhead_execution.pdf", bbox_inches='tight')


def storage_overhead(results, output_sizes, checkpoint_sizes):
    # breakpoint()
    # print(output_sizes)
    data = []
    for o in output_sizes:
        algorithm = o.withLineage.algorithm
        graph = o.withLineage.graph

        lineageDir = results[(algorithm, graph)].withLineage[0].metadata.lineageDirectory

        checkpoint_size = [x for x in checkpoint_sizes if x.lineageId == lineageDir][0]

        lineageSize = sum(size.size for size in checkpoint_size.iterations)

        iterations = len(results[(algorithm, graph)].withLineage[0].metadata.iterations)
        it = iterations if iterations > 0 else 1

        fraction = (lineageSize / it) / o.withoutLineage.size
        data.append((algorithm, graph, fraction))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    bar_width = 1 / (num_algorithms + 1)

    _, ax = plt.subplots()

    for idx, algorithm in enumerate(algorithms):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for dataset in datasets]
        positions = [i + idx * bar_width for i in range(num_datasets)]
        bars = ax.bar(positions, sizes, width=bar_width, label=algorithm_names_short[algorithm])

        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval, f"{yval:.2f}", va='bottom', ha='center')


    ax.set_xlabel("Dataset")
    ax.set_xticks([i + bar_width * (num_algorithms - 1) / 2 for i in range(num_datasets)])
    ax.set_xticklabels(datasets)
    ax.set_ylabel("Data multiplication factor (per Pregel iteration)")
    # ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: pretty_size(y)))
    ax.legend()
    plt.savefig(plots_dir / "overhead_storage.pdf", bbox_inches='tight')

