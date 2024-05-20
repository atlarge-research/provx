#!/usr/bin/env bash

set -euo pipefail

cd /home/gmo520/provxlib/run

export RUNNER_CONFIG="{{ .ExperimentConfigPath }}"

export ENV_FILE="env-$SLURM_JOB_ID"

just setup
just dry
just bench "benchmark"
