
import threading 
from constants import TIME_ARRAY_OUT_DIRECTORY, H5_FILE_DIRECTORY, H5_CHUNK_OUT_DIRECTORY
from hdf5_chunk_writer import chunk_output, get_h5_file_paths
from inotify_chunk_deleter import delete_old_chunks_on_new_dir_creation
from mpi4py import MPI
from time import time_ns
H5_FILE_LIST = get_h5_file_paths(H5_FILE_DIRECTORY)
import os
import numpy as np

def run_with_mpi(comm, rank, cores, current_time):
    global H5_CHUNK_OUT_DIRECTORY, TIME_ARRAY_OUT_DIRECTORY
    
    H5_CHUNK_OUT_DIRECTORY = os.path.join(H5_CHUNK_OUT_DIRECTORY, f"run_at_{current_time}")
    TIME_ARRAY_OUT_DIRECTORY = os.path.join(TIME_ARRAY_OUT_DIRECTORY, f"run_at_{current_time}_time_results.npy")

    if rank == 0:
        print("core running inotify started")
        delete_old_chunks_on_new_dir_creation(H5_CHUNK_OUT_DIRECTORY)
    elif rank == 1:
        print("core to save numpy array active")
        get_times_from_other_processes(comm, cores, TIME_ARRAY_OUT_DIRECTORY)

    else: 
        print("core outputting chunks started")
        chunk_output(comm, rank, H5_CHUNK_OUT_DIRECTORY, H5_FILE_LIST)


def get_start_time(comm, rank, cores):
    """
    Current time so cores can use the same autogenerated directory.
    """
    current_time = time_ns()

    if rank == 0:
        for rank in range(cores):
            if rank != 0:
                comm.isend(current_time, dest=rank, tag=1)
        return current_time

    else:
        return comm.recv(source=0, tag=1)

def save_times(times_array, times_file_path):
    np.save(times_file_path, times_array)

def get_times_from_other_processes(comm, cores, times_path, default_array_size=100000):
    times_array = np.zeros(default_array_size)

    for _ in range(default_array_size):
        for core in range(cores - 2):
            new_time = comm.recv(source=core+2, tag=3)
            times_array = np.append(times_array, new_time)

            print(f"recieved time {new_time} from core {core}")
        save_times(times_array, times_path)


def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    cores = comm.Get_size()

    if cores < 3:
        print("MPI can't find the three cores needed, run with mpiexec -n 3")
        return

    print(f"running with {cores} mpi cores")

    # Get the time core 0 started
    current_time = get_start_time(comm, rank, cores)

    run_with_mpi(comm, rank, cores, current_time)

if __name__ == "__main__":
    main()