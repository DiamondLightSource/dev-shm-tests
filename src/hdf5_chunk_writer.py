"""
Starts a run to write chunks to /dev/shm/run_num_x in the form of .dat files. Will log the time taken for a run, and when a run finishes another
will be started.
If /dev/shm handles fragmentation well then, we wont see a time decrease as inotify_chunk_deleter.py continues to delete chunks from a each new run.
"""

import h5py
import numpy as np
import os
from time import time_ns

def new_run(rank, h5_save_path, h5_load_file_path, run_number):

    # New directory to store run.
    h5_save_path_this_run = os.path.join(h5_save_path, f"run_num_{run_number}_rank_{rank}")
    print(f"starting new run {run_number}")
    os.makedirs(h5_save_path_this_run)


    with h5py.File(h5_load_file_path) as f:
        data = f["data"]
        chunks = []

        # Read chunks into memory.
        for chunk_num in range(data.shape[0]):
            chunks.append(data.id.read_direct_chunk((chunk_num, 0, 0))[1])
        
        # We only want to time write, not read.
        start_time = time_ns()
        for i  in range(len(chunks)):
            with open(os.path.join(h5_save_path_this_run,f"chunk_{i}.dat"), "wb") as f:
                f.write(chunks[i])
        end_time = time_ns()
    return end_time - start_time




def chunk_output(comm, rank, h5_save_path, h5_load_paths, max_iterations=50000):

    for iteration in range(max_iterations):
        # Add the rank so cores aren't reading from the same h5 file.
        time_taken = new_run(rank, h5_save_path, h5_load_paths[(iteration + rank) % len(h5_load_paths)], iteration)


        # Pass it to the core saving to the numpy array
        comm.isend(time_taken, dest=1, tag=3)

def get_h5_file_paths(path):
    path_filenames = os.listdir(path)
    h5_files = [os.path.join(path, filename) for filename in path_filenames if filename[-3:] == ".h5"] 
    
    # Check to see if all the hdf5 files have data and don't use the ones which don't in out list.
    for h5_file in h5_files:
        try:
            f = h5py.File(h5_file, "r")
            data = f["data"]
        except KeyError: # we want to skip over files without data
            del h5_files[h5_files.index(h5_file)]

    return h5_files