from typing import List, Dict, Tuple
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import numpy as np

from schema import FullResult, FullOutputSize, CheckpointSize
from config import plots_format


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

def messages_per_iteration(results: Dict[Tuple[str, str], FullResult], archive_dir: Path):
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
        plt.savefig(archive_dir / f"{algorithm}-{graph}.{plots_format}", dpi=300)
        plt.tight_layout()
        plt.clf()


def execution_overhead(results: Dict[Tuple[str, str], FullResult], archive_dir: Path, single: bool = False):
    data = []
    for (algorithm, dataset), r in results.items():
        if dataset == "graph500-22":
            continue
        per_run = []
        for idx, wl in enumerate(r.withLineage):
            wl_duration = wl.metadata.duration.duration
            wol_duration = r.withoutLineage[idx].metadata.duration.duration

            #if len(wl.metadata.iterations) == 0:
            #    print("WARN: no iterations for", algorithm, dataset)
            #    continue

            overhead = wl_duration / wol_duration
            if single:
                overhead /= len(wl.metadata.iterations)
            overhead *= 100
            if overhead > 100:
                overhead -= 100

            per_run.append(overhead)
        mean = np.mean(per_run)
        std = np.std(per_run)
        data.append((algorithm, dataset, mean, std))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    #bar_width = 1 / (num_algorithms + 1)
    bar_width = 1 / (num_datasets + 1)

    _, ax = plt.subplots()

    # for idx, algorithm in enumerate(algorithms):
    #     sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
    #              for dataset in datasets]
    #     errors = [next((entry[3] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
    #              for dataset in datasets]
    #     positions = [i + idx * bar_width for i in range(num_datasets)]
    #     ax.bar(positions, sizes, yerr=errors, width=bar_width, label=algorithm_names_short[algorithm], capsize=2, align='center')

    for idx, dataset in enumerate(datasets):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        errors = [next((entry[3] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        positions = [i + idx * bar_width for i in range(num_algorithms)]
        ax.bar(positions, sizes, yerr=errors, width=bar_width, label=dataset, capsize=2, align='center')

    ax.set_title('Execution overhead of lineage storage' + (" (single iteration)" if single else ""))
    ax.set_ylabel('Overhead compared to non-lineage execution')
    ax.set_xlabel("Algorithm")
    #ax.set_xlabel("Dataset")
    #ax.set_xticks([i + bar_width * (num_algorithms - 1) / 2 for i in range(num_datasets)])
    #ax.set_xticklabels(datasets, rotation=45, ha='right')
    ax.set_xticks([i + bar_width * (num_datasets - 1) / 2 for i in range(num_algorithms)])
    ax.set_xticklabels([algorithm_names_short[a] for a in algorithms], rotation=45, ha='right')
    #ax.set_xticklabels(algorithms, rotation=45, ha='right')
    ax.yaxis.set_major_formatter(ticker.PercentFormatter())
    # ax.legend()

    pos = ax.get_position()
    ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
    ax.legend(loc='center right', bbox_to_anchor=(1.4, 0.5))


    filename = "overhead_execution" + ("_single" if single else "")
    plt.savefig(archive_dir / f"{filename}.{plots_format}", dpi=600, bbox_inches='tight')

def iterations_per_dataset(results: Dict[Tuple[str, str], FullResult], archive_dir: Path):
    data = []
    for (algorithm, dataset), r in results.items():
        if dataset == "graph500-22":
            continue
        per_run = []
        for wl in r.withLineage:
            iterations = len(wl.metadata.iterations)
            per_run.append(iterations)
        mean = np.mean(per_run)
        std = np.std(per_run)
        data.append((algorithm, dataset, mean, std))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    # bar_width = 1 / (num_algorithms + 1)
    bar_width = 1 / (num_datasets + 1)

    _, ax = plt.subplots()

    # for idx, algorithm in enumerate(algorithms):
    #     sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
    #              for dataset in datasets]
    #     positions = [i + idx * bar_width for i in range(num_datasets)]
    #     ax.bar(positions, sizes, width=bar_width, label=algorithm_names_short[algorithm])

    for idx, dataset in enumerate(datasets):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        positions = [i + idx * bar_width for i in range(num_algorithms)]
        ax.bar(positions, sizes, width=bar_width, label=dataset)

    ax.set_title('Iterations per dataset')
    ax.set_ylabel('Iterations')
    ax.set_xlabel("Dataset")
    ax.set_xticks([i + bar_width * (num_datasets - 1) / 2 for i in range(num_algorithms)])
    # ax.set_xticks([i + bar_width * (num_algorithms - 1) / 2 for i in range(num_datasets)])
    # ax.set_xticklabels(datasets, rotation=45, ha='right')
    ax.set_xticklabels([algorithm_names_short[a] for a in algorithms], rotation=45, ha='right')

    # ax.legend()

    plt.savefig(archive_dir / f"iterations_per_dataset.{plots_format}", dpi=600, bbox_inches='tight')


def storage_overhead(
        results: Dict[Tuple[str, str], FullResult],
        output_sizes: List[FullOutputSize],
        checkpoint_sizes: List[CheckpointSize],
        archive_dir: Path,
        single: bool = False):
    data = []
    for o in output_sizes:
        algorithm = o.withLineage.algorithm
        graph = o.withLineage.graph

        lineageDir = results[(algorithm, graph)].withLineage[0].metadata.lineageDirectory

        checkpoint_size = [x for x in checkpoint_sizes if x.lineageId == lineageDir][0]

        lineageSize = sum(size.size for size in checkpoint_size.iterations)

        # iterations = len(results[(algorithm, graph)].withLineage[0].metadata.iterations)
        # it = iterations if iterations > 0 else 1

        fraction = lineageSize / o.withoutLineage.size
        data.append((algorithm, graph, fraction))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    bar_width = 1 / (num_datasets + 1)

    _, ax = plt.subplots()

    # for idx, algorithm in enumerate(algorithms):
    #     sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
    #              for dataset in datasets]
    #     positions = [i + idx * bar_width for i in range(num_datasets)]
    #     bars = ax.bar(positions, sizes, width=bar_width, label=algorithm_names_short[algorithm])
    #
    #     for bar in bars:
    #         yval = bar.get_height()
    #         plt.text(bar.get_x() + bar.get_width()/2, yval, f"{yval:.2f}", va='bottom', ha='center')

    ax.set_ylim([0, 40])

    for idx, dataset in enumerate(datasets):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        positions = [i + idx * bar_width for i in range(num_algorithms)]
        bars = ax.bar(positions, sizes, width=bar_width, label=dataset)

        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval+0.2, f"{yval:.2f}", va='bottom', ha='center', rotation=90)

    ax.set_title("Storage overhead for lineage data (compared to algorithm output)")
    ax.set_xlabel("Algorithm")
    ax.set_xticks([i + bar_width * (num_datasets - 1) / 2 for i in range(num_algorithms)])
    ax.set_xticklabels([algorithm_names_short[a] for a in algorithms], rotation=45, ha='right')
    ax.set_ylabel("Data multiplication factor")
    # ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: pretty_size(y)))
    # ax.legend()


    pos = ax.get_position()
    ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
    ax.legend(loc='center right', bbox_to_anchor=(1.4, 0.5))

    plt.savefig(archive_dir / f"overhead_storage.{plots_format}", dpi=300, bbox_inches='tight')

