# ProvX: A Multi-Granularity Provenance System for Graph Processing Workflows

## Motivation
In modern data ecosystems, tracking data transformations is critical for ensuring integrity and compliance. Graph processing systems are powerful for analyzing connected data, but tracking their changes is complex. Provenance, which traces data lineage, helps manage this complexity by tracking graph transformations while enabling debugging and performance analysis.

ProvX introduces a reference design for integrating provenance into graph processing systems, providing fine-grained control over operations. It features a provenance query API for customizable data capture and a metrics system for monitoring performance. We demonstrate that users can control both the data captured and the overhead of provenance tracking.

## Setup

### Repository structure

| Directory | Description |
| --- | --- |
| `lib` | ProvX reference design implemented on top of [Apache Spark](https://spark.apache.org)'s [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html). |
| `metarunner` | Parameterisable [Magpie](https://github.com/LLNL/magpie) configuration generator for running experiments on differently sized Apache Spark clusters. |
| `results` | Jupyter notebooks to plot results. |
| `run` | Experiment working directory and results location. |

### Pre-requisites

- [Scala](https://www.scala-lang.org/) 2.13 (with JDK11) used to compile ProvX library for GraphX.
- Apache Spark (3.3.2 with support for Scala 2.13) installation with Hadoop 3.2.4
- SLURM cluster to run Apache Spark (via Magpie scripts).
- [Go](https://go.dev/) 1.23 for compiling `metarunner` program.
- [Python](https://www.python.org/) 3.10 for plotting the experiment results (`results` directory)
- [`just`](https://just.systems/) command runner

## Experiment cluster setup

### Local machine setup

1. Install the appropriate Python, Go and Scala versions.
2. Compile `lib` by running `just build` in the `lib` directory. This will produce the JAR file at `./lib/target/scala-2.13/provxlib-assembly-0.1.0-SNAPSHOT.jar`.
3. Compile `metarunner` by running `just build` in the `metarunner` directory. Make sure to adjust the compilation flags depending on the target architecture of your SLURM cluster machines.
4. `rsync` this repo (with the compiled library and metarunner) to your SLURM cluster.

### SLURM cluster setup

1. Download and extract Apache Spark and Hadoop
```
$ wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3-scala2.13.tgz
$ tar xvzf spark-3.3.2-bin-hadoop3-scala2.13.tgz
$ wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz
$ tar xvzf hadoop-3.2.4.tar.gz
```
2. Clone Magpie repo
```sh
$ git clone git@github.com:LLNL/magpie.git
```
3. Patch Apache Spark installation
```
$ cd spark-3.3.2-bin-hadoop3-scala2.13
$ patch -p1 < <magpie-repo-root>/patches/spark/spark-3.3.2-bin-hadoop3-alternate.patch
```

## Reproducting results

:fire: Make sure you followed all steps in the previous section! :fire:

### Setting up the Apache Spark cluster
1. Update the experiment configurations in `metarunner/configs` to point to the correct JAR location for the ProvX library.
2. Adjust `metarunner/templates/magpie.sbatch-srun-provx-with-yarn-and-hdfs` to suit your SLURM cluster's configuration.
3. Generate the launch scripts by running the metarunner: `./metarunner -configDir ./configs -outputDir ./scripts`. In the `scripts` directory you will find the `sbatch` job scripts for setting up the Apache Spark cluster on your SLURM cluster. You can submit them individually via, e.g. `sbatch magpie.sbatch-srun-provx-es01-baseline-06.sh` depending on which experiment you want to reproduce.

## Running the experiments

1. SSH into the Apache Spark's headnode (Check the submitted job file for the line starting with `#SBATCH --output=` to determine which output file the job is writing to. This file will contain the hostname of the Spark headnode).
2. Change into this repo's `run` directory on the Spark headnode.
3. Update the `justfile` depending on your setup.
4. Run the following commands to setup the environment variables, download and copy the datasets from Graphalytics onto Hadoop and do a dry-run to determine if everything is setup properly:
```
$ just env
$ source ./env
$ just setup
$ just dry
```
5. Finally, the experiment can be executed using: `just bench "<experiment description>"`

The results can be found in the location configured in the `justfile` in the `run` directory.
