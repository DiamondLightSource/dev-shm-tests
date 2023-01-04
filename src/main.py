from constants import TIME_ARRAY_OUT_DIRECTORY, H5_FILE_DIRECTORY, H5_CHUNK_OUT_DIRECTORY
from hdf5_chunk_writer import chunk_output, get_h5_file_paths
from inotify_chunk_deleter import delete_old_chunks_on_new_dir_creation
from mpi4py import MPI
from time import time_ns
import os

H5_FILE_LIST = get_h5_file_paths(H5_FILE_DIRECTORY)

def run_with_mpi(comm, rank, current_time):
    global H5_CHUNK_OUT_DIRECTORY, TIME_ARRAY_OUT_DIRECTORY

    
    h5_chunk_out_directory = os.path.join(H5_CHUNK_OUT_DIRECTORY, f"run_at_{current_time}")
    time_array_out_directory = os.path.join(TIME_ARRAY_OUT_DIRECTORY, f"run_at_{current_time}_time_results.npy")

    if rank == 0:
        try:
            os.mkdir(TIME_ARRAY_OUT_DIRECTORY)
        except FileExistsError:
            pass 

        print("core running inotify started")
        delete_old_chunks_on_new_dir_creation(h5_chunk_out_directory)
    elif rank == 1:
        print("core outputting chunks started")
        chunk_output(h5_chunk_out_directory, H5_FILE_LIST, time_array_out_directory)


def get_start_time(comm, rank, cores):
    """
    Current time so cores can use the same autogenerated directory.
    """
    current_time = time_ns()
    if cores == 1:
        return current_time
    if rank == 0:
        comm.isend(current_time, dest=1, tag=1)
        return current_time
    if rank == 1:
        return comm.recv(source=0, tag=1)



def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    cores = comm.Get_size()

    if cores < 2:
        print("MPI can't find two cores, run with mpiexec -n 2")
        return

    print("running with mpi cores")

    # Get the time core 0 started
    current_time = get_start_time(comm, rank, cores)

    run_with_mpi(comm, rank, current_time)

if __name__ == "__main__":
    main()