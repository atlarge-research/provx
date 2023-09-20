import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from config import plots_dir, results_dir

from pathlib import Path

from schema import Model
import json
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

# Nodes size
graph_sizes = {
    "kgs": 5714619,
    "wiki-Talk": 18043970,
    "cit-Patents": 30025298,
}

def format_seconds(x, pos):
    hours = int(x // 3600)
    minutes = int((x % 3600) // 60)
    seconds = int(x % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_filesize(x, pos):
    # Define size units
    size_units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    unit_index = 0

    # Convert bytes to higher units as needed
    while x >= 1024 and unit_index < len(size_units) - 1:
        x /= 1024
        unit_index += 1

    return f"{x:.2f} {size_units[unit_index]}"


def values_plot(raw_data, configuration, output_dir: Path, metric="duration"):
    baseline = raw_data["baseline"]
    c = raw_data[configuration]
    data = []
    for (algorithm, dataset) in baseline.keys():
        comparison_runs = c[(algorithm, dataset)]
        values = [m[metric] for m in comparison_runs]
        data.append((algorithm, dataset, np.mean(values), np.std(values)))

    algorithms = sorted(list(set([entry[0] for entry in data])))
    datasets = sorted(list(set([entry[1] for entry in data])))

    num_algorithms = len(algorithms)
    num_datasets = len(datasets)

    #bar_width = 1 / (num_algorithms + 1)
    bar_width = 1 / (num_datasets + 1)

    _, ax = plt.subplots()

    for idx, dataset in enumerate(datasets):
        sizes = [next((entry[2] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        errors = [next((entry[3] for entry in data if entry[0] == algorithm and entry[1] == dataset), 0)
                 for algorithm in algorithms]
        positions = [i + idx * bar_width for i in range(num_algorithms)]
        ax.bar(positions, sizes, yerr=errors, width=bar_width, label=dataset, capsize=2, align='center')

    # ax.set_title('Execution overhead of lineage storage' + (" (single iteration)" if single else ""))
    ax.set_title(f"{metric} values for {configuration}")
    ax.set_xlabel("Algorithm")
    ax.set_xticks([i + bar_width * (num_datasets - 1) / 2 for i in range(num_algorithms)])
    ax.set_xticklabels([algorithm_names_short[a.lower()] for a in algorithms])

    if metric == "duration":
        y_min = 0
        y_max = 300

        num_ticks = 10  # or any desired number of ticks
        y_ticks = np.linspace(y_min, y_max, num_ticks)
        ax.set_yticks(y_ticks)
        ax.yaxis.set_major_formatter(FuncFormatter(format_seconds))
    elif metric == "total_size":
        y_min = 0
        y_max = 2**29

        num_ticks = 10  # or any desired number of ticks
        y_ticks = np.linspace(y_min, y_max, num_ticks)
        ax.set_yticks(y_ticks)
        ax.yaxis.set_major_formatter(FuncFormatter(format_filesize))

    pos = ax.get_position()
    ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5))

    plots_format = "pdf"
    filename = f"values_{configuration}_{metric}"
    plt.savefig(output_dir / f"{filename}.{plots_format}", dpi=600, bbox_inches='tight')
    plt.close()

def overhead_plot(raw_data, configuration, output_dir: Path, metric="duration"):
    baseline = raw_data["baseline"]
    c = raw_data[configuration]
    data = []
    for (algorithm, dataset), base in baseline.items():
        comparison_runs = c[(algorithm, dataset)]
        values = [m[metric] for m in comparison_runs]
        mean_baseline = np.mean([b[metric] for b in base])
        mean = np.mean(values) / mean_baseline
        std = np.std(values) / mean_baseline
        if metric == "duration":
            print(f"{algorithm}, {dataset} -> {configuration}, {metric}")
            print(f"baseline: {mean_baseline}, compared to {np.mean(values)}: {mean:.2f}x\n")
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
    # if metric != "total_size":
    # ax.yaxis.set_major_formatter(ticker.PercentFormatter())
    # ax.legend()

    pos = ax.get_position()
    ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
    # ax.legend(loc='center right')
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5))

    plots_format = "pdf"
    filename = f"overhead_{configuration}_{metric}"
    plt.savefig(output_dir / f"{filename}.{plots_format}", dpi=600, bbox_inches='tight')
    plt.close()


def main():
    experiments_dirs = list([d for d in results_dir.iterdir() if d.is_dir()])
    latest_experiment = sorted(experiments_dirs, key=lambda x: x.name)[-1]
    input_files = sorted(latest_experiment.glob("experiment-*/provenance.json"))
    provenance_results = [Model(**json.loads(f.read_text())) for f in input_files]

    data = {}
    for r in provenance_results:
        algorithm = r.inputs.parameters.algorithm
        dataset = r.inputs.parameters.dataset
        config = r.inputs.parameters.setup.lower()
        if config not in data:
            data[config] = {}
        c = data[config]
        k = (algorithm, dataset)
        if c.get(k) is None:
            c[k] = []
        total_size = sum([l.size for l in r.outputs.sizes.individual])
        c[k].append({
            "run": r.inputs.parameters.runNr,
            "duration": r.outputs.duration.amount / 10**9, # in seconds
            "output_size": r.outputs.sizes.total,
            "total_size": (total_size if config != "baseline" else r.outputs.sizes.total)
        })

    for setup in data.keys():
        pdir = plots_dir / latest_experiment.name / setup
        pdir.mkdir(exist_ok=True, parents=True)

        values_plot(data, setup, pdir, metric="duration")
        values_plot(data, setup, pdir, metric="total_size")

        if setup == "baseline":
            continue

        overhead_plot(data, setup, pdir, metric="duration")
        overhead_plot(data, setup, pdir, metric="total_size")


if __name__ == '__main__':
    main()
