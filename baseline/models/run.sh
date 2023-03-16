#! /bin/bash
srun -K \
--job-name="forc-baselines-exp" \
--partition=RTX2080Ti \
--nodes=1 \
--ntasks=1 \
--mem=128G \
--cpus-per-task=4 \
--gpus-per-task=1 \
--container-image=/netscratch/$USER/my_name.sqsh \
--container-workdir="`pwd`" \
--container-mounts=/netscratch/$USER:/netscratch/$USER,/ds:/ds:ro,"`pwd`":"`pwd`" \
python baseline_models_exp.py