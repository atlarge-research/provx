#!/usr/bin/env bash

set -euo pipefail

cd /home/gmo520/provxlib/run

export RUNNER_CONFIG="{{ .ExperimentConfigPath }}"

just setup
just dry
just bench "benchmark"
