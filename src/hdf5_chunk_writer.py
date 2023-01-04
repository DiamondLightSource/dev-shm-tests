"""
Starts a run to write chunks to /dev/shm/run_num_x in the form of .dat files. Will log the time taken for a run, and when a run finishes another
will be started.
If /dev/shm handles fragmentation well then, we wont see a time decrease as inotify_chunk_deleter.py continues to delete chunks from a each new run.
"""

import h5py
import numpy as np
import os
from time import time_ns, sleep

def new_run(h5_save_path, h5_load_file_path, run_number):

    # New directory to store run.
    h5_save_path_this_run = os.path.join(h5_save_path, f"run_num_{run_number}")
    os.makedirs(h5_save_path_this_run)


    with h5py.File(h5_load_file_path) as f:
        data = f["data"]
        for chunk_num in range(data.shape[0]):
            chunk = data.id.read_direct_chunk((chunk_num, 0, 0))[1]
            with open(os.path.join(h5_save_path_this_run,f"chunk_{chunk_num}.dat"), "wb") as f:
                f.write(chunk)




def save_times(times_array, times_file_path):
    np.save(times_file_path, times_array)

def chunk_output(h5_save_path, h5_load_paths, times_path, storage_file_name, save_after_iterations=5, max_iterations=50000):


    times_file_path = os.path.join(times_path, f"{storage_file_name}.npy")
    times_array = np.zeros(max_iterations)

    for iteration in range(max_iterations):
        run_start_time = time_ns()
        new_run(h5_save_path, h5_load_paths[iteration % len(h5_load_paths)], iteration)
        run_end_time = time_ns()

        times_array[iteration] = run_end_time - run_start_time

        if (iteration + 1) % save_after_iterations == 0:
            print(f"time array: another {save_after_iterations} runs complete, saving results to {times_file_path}")
            save_times(times_array, times_file_path)


def get_h5_file_paths(path):
    path_filenames = os.listdir(path)
    h5_files = [os.path.join(path, filename) for filename in path_filenames if filename[-3:] == ".h5"] 
    
    # Check to see if all the hdf5 files have data and delete ones which don't.
    for h5_file in h5_files:
        try:
            f = h5py.File(h5_file, "r")
            data = f["data"]
        except KeyError: # we want to skip over files without data
            del h5_files[h5_files.index(h5_file)]

    return h5_files