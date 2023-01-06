#!/bin/bash
module load python/3.10
if [ -d "./.venv" ]
then
rm -rf .venv
fi
mkdir .venv

python -m venv .venv
.venv/bin/pip install --upgrade pip
module load mpich
.venv/bin/pip install h5py inotify mpi4py matplotlib pyzmq
