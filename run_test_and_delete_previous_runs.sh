#!/bin/bash
module load mpich
ulimit -c unlimited
rm -rf out/*.npy /dev/shm/run_at* && mpiexec -n 3 .venv/bin/python src/main.py
