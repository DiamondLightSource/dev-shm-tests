#!/bin/bash
module load python/3.10
if [ -d "./.venv" ]
then
rm -rf .venv
fi
mkdir .venv

python -m venv .venv
module load mpich
.venv/bin/pip install --upgrade pip 
.venv/bin/pip install h5py inotify mpi4py matplotlib pyzmq black flake8
