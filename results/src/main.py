from pathlib import Path
import pandas as pd
import json
from schema import Model

import seaborn as sns
from matplotlib import pyplot as plt

def parse_experiment_output(root_folder):
    input_files = sorted(root_folder.glob("experiment-*/provenance/inputs.json"))
    output_files = sorted(root_folder.glob("experiment-*/provenance/outputs.json"))
    provenance_results = [
        Model(
            inputs=json.loads(f.read_text()),
            outputs=json.loads(output_files[idx].read_text())
        ) for idx, f in enumerate(input_files)
    ]

    data = []
    for r in provenance_results:
        config = r.inputs.parameters.setup.lower()
        total_size = sum([l.size for l in r.outputs.sizes.individual])
        data.append({
            "config": config,
            "algorithm": r.inputs.parameters.algorithm,
            "dataset": r.inputs.parameters.dataset,
            "run": r.inputs.parameters.runNr,
            "storage_format": r.inputs.parameters.storageFormat,
            "duration": r.outputs.duration.amount / 10**9, # in seconds
            "output_size": r.outputs.sizes.total,
            "total_size": (total_size if config != "baseline" else r.outputs.sizes.total),
            "nr_executors": r.inputs.parameters.executorCount,
        })
    return pd.DataFrame(data)

def main():
    path = Path(__file__).parent.parent / "data" / "das6" / "storageformats-scaling"
    results = parse_experiment_output(path)
    sns.boxplot(results[results["nr_executors"] == 1], x="duration", y="storage_format", hue="algorithm")
    plt.show()
    breakpoint()

if __name__ == '__main__':
    main()
