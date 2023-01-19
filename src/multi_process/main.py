from constants import USE_CIRCULAR_BUFFER, FACTORS_OF_CHUNKS_TO_DELETE
from hdf5_chunk_writer import DirectoryBuffer, CircularBuffer
from inotify_chunk_deleter import DirectoryDeleter
from mpi4py import MPI
from zmq_socket import ZMQServer


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
            writer = CircularBuffer()
            writer.write_chunks_loop()

        else:
            writer = DirectoryBuffer()
            # Start the chunk deleter now that the new directory has been created for it.
            comm.send(writer.run_directory, dest=2, tag=0)
            writer.write_chunks_loop()

    elif not USE_CIRCULAR_BUFFER and rank == 2:
        # Wait for process two to make the run directory before we watch to delete.
        run_directory = comm.recv(source=1, tag=0)

        DirectoryDeleter(
            run_directory, factors_of_chunks_to_delete=FACTORS_OF_CHUNKS_TO_DELETE
        )

    else:
        print(f"Core {rank} has nothing to do.")


if __name__ == "__main__":
    main()
