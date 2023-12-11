#!/usr/bin/env bash

configuration=$1

for f in $(ls -1 "scripts/magpie.sbatch-srun-provx-${configuration}"*); do
	jid=$(sbatch $f | awk '{ print $4 }')
	echo Running job $jid
	sleep 1
	while true; do
		if ! squeue -u $USER | awk '{print $1}' | grep "$jid" > /dev/null; then
			break
		fi
		sleep 5
	done
	echo Finished running job $jid
done

