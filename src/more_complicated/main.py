from constants import (
    USE_CIRCULAR_BUFFER,
)
from hdf5_chunk_writer import DirectoryBuffer, CircularBuffer
from inotify_chunk_deleter import delete_old_chunks_on_new_dir_creation
from mpi4py import MPI
from zmq_socket import ZMQServer
import time


def check_enough_cores(rank, cores):
    # One less core required if we're using a circular buffer.
    cores_required = 3 - USE_CIRCULAR_BUFFER

    if cores < cores_required:
        if rank == 0:
            print(
                f"Test is using {cores} cores, requires at least {cores_required}, run with `mpiexec -n {cores_required}`."
            )
        exit()

    if rank == 0:
        print(f"Running with {cores} cores")


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    cores = comm.Get_size()

    check_enough_cores(rank, cores)

    if rank == 0:
        ZMQServer()

    elif rank == 1:
        if USE_CIRCULAR_BUFFER:
            CircularBuffer()
        else:
            DirectoryBuffer()

    elif not USE_CIRCULAR_BUFFER and rank == 2:
        comm.recv(source=1, tag=0)
        delete_old_chunks_on_new_dir_creation()

    else:
        # Sleep for the new directory to be created
        time.sleep(3)
        print(f"Core {rank} has nothing to do.")


if __name__ == "__main__":
    main()
