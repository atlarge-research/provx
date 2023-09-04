from typing import Literal

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from config import plots_dir, results_dir
# from schema import CheckpointSize, FullResult
from utils import pretty_size
# import ingest 
# import plots


# def graph_size_per_iteration(results: Dict[Tuple[str, str], FullResult], checkpoints: List[CheckpointSize]):
#
#     for (algorithm, graph), v in results.items():
#         if v.withLineage.metadata.lineageDirectory is None:
#             continue
#
#         lineageDirectory = v.withLineage.metadata.lineageDirectory
#         iterations = [checkpoint for checkpoint in checkpoints if checkpoint.lineageId == lineageDirectory]
#         print(iterations)
#
#         print(algorithm, graph, v.withLineage.metadata.lineageDirectory)
#     return

    # indices = list(range(len(sizes)))
    # labels = [str(index) for index in indices]
    #
    # fig, ax = plt.subplots()
    #
    # ax.bar(indices, sizes)
    # ax.set_xlabel("Index")
    # ax.set_ylabel("Size")
    # ax.set_title(f"Sizes for {dataset_name} using {algorithm_name}")
    #
    # # Format y-axis to display human-readable sizes
    # ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: pretty_size(y)))
    #
    # for index, size in zip(indices, sizes):
    #     plt.text(index, size, str(index), ha="center", va="bottom")
    #
    # plt.show()


from schema import Model
import json

def determine_configuration(model: Model) -> Literal["baseline", "tracing", "storage", "compression"]:
    params = model.inputs.parameters
    if not params.lineageEnabled:
        return "baseline"
    elif params.lineageEnabled and not params.compressionEnabled and not params.storageEnabled:
        return "tracing"
    elif params.lineageEnabled and not params.compressionEnabled and params.storageEnabled:
        return "storage"
    elif params.lineageEnabled and params.compressionEnabled and params.storageEnabled:
        return "compression"

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

def overhead_plot(raw_data, configuration, metric="duration"):
    baseline = raw_data["baseline"]
    c = raw_data[configuration]
    data = []
    for (algorithm, dataset), base in baseline.items():
        comparison = c[(algorithm, dataset)]
        overhead = (comparison[metric] / base[metric]) * 100
        print(overhead)
        # if overhead > 100:
        #     overhead -= 100
        # per_run = []
        # for idx, wl in enumerate(r.withLineage):
        #     if single:
        #         overhead /= len(wl.metadata.iterations)
        #     overhead *= 100
        #     if overhead > 100:
        #         overhead -= 100
        #
        #     per_run.append(overhead)
        # mean = np.mean(per_run)
        # std = np.std(per_run)
        # data.append((algorithm, dataset, mean, std))
        data.append((algorithm, dataset, overhead, 0.0))

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

    # ax.set_title('Execution overhead of lineage storage' + (" (single iteration)" if single else ""))
    ax.set_title(f"{metric} overhead for {configuration}")
    ax.set_ylabel('Overhead compared to baseline execution')
    ax.set_xlabel("Algorithm")
    #ax.set_xlabel("Dataset")
    #ax.set_xticks([i + bar_width * (num_algorithms - 1) / 2 for i in range(num_datasets)])
    #ax.set_xticklabels(datasets, rotation=45, ha='right')
    ax.set_xticks([i + bar_width * (num_datasets - 1) / 2 for i in range(num_algorithms)])
    # ax.set_xticklabels([algorithm_names_short[a.lower()] for a in algorithms], rotation=45, ha='right')
    ax.set_xticklabels([algorithm_names_short[a.lower()] for a in algorithms])
    #ax.set_xticklabels(algorithms, rotation=45, ha='right')
    if metric != "total_size":
        ax.yaxis.set_major_formatter(ticker.PercentFormatter())
    # ax.legend()

    pos = ax.get_position()
    ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
    ax.legend(loc='center right', bbox_to_anchor=(1.4, 0.5))

    plots_format = "pdf"
    filename = f"overhead_{configuration}_{metric}"
    plt.savefig(plots_dir / "20230903" / f"{filename}.{plots_format}", dpi=600, bbox_inches='tight')


def main():
    input_files = sorted(results_dir.glob("experiment-*/provenance.json"))
    provenance_results = [Model(**json.loads(f.read_text())) for f in input_files]

    data = {
        "baseline": {},
        "tracing": {},
        "storage": {},
        "compression": {}
    }
    for r in provenance_results:
        algorithm = r.inputs.parameters.algorithm
        dataset = r.inputs.parameters.dataset
        config = determine_configuration(r)
        c = data[config]
        k = (algorithm, dataset)
        if c.get(k) is None:
            total_size = sum([l.size for l in r.outputs.sizes.individual[0]])
            c[k] = {
                # "algorithm": r.inputs.parameters.algorithm,
                # "dataset": r.inputs.parameters.dataset,
                "duration": r.outputs.duration.amount / 10**9,
                "output_size": r.outputs.sizes.total / 1024 / 1024,
                "total_size": (total_size if config != "baseline" else r.outputs.sizes.total) / 1024 / 1024
            }

    overhead_plot(data, "tracing", metric="duration")
    overhead_plot(data, "storage", metric="duration")
    overhead_plot(data, "compression", metric="duration")
    overhead_plot(data, "tracing", metric="total_size")
    overhead_plot(data, "storage", metric="total_size")
    overhead_plot(data, "compression", metric="total_size")
    # print(data)

    # for archive_dir in results_dir.iterdir():
    #     if archive_dir.is_file():
    #         continue
    #
    #     print(archive_dir)
    #     print(input_files)

        # results = ingest.load_results(archive_dir)
        # output_sizes = ingest.parse_output_sizes(archive_dir)
        # checkpoint_sizes = ingest.parse_checkpoint_sizes(archive_dir)
        #
        # p_dir = plots_dir / archive_dir.name
        # if not p_dir.exists():
        #     p_dir.mkdir()
        #
        # plots.execution_overhead(results, p_dir)
        # plots.execution_overhead(results, p_dir, single=True)
        # plots.iterations_per_dataset(results, p_dir)
        #
        # plots.storage_overhead(results, output_sizes, checkpoint_sizes, p_dir)

        #plots.messages_per_iteration(results)

        # graph_size_per_iteration(results, checkpoint_sizes)

if __name__ == '__main__':
    main()
