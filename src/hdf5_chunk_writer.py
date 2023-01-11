"""
Starts a run to write chunks to /dev/shm/run_num_x in the form of .dat files. Will log the time taken for a run, and when a run finishes another
will be started.
If /dev/shm handles fragmentation well then, we wont see a time decrease as inotify_chunk_deleter.py continues to delete chunks from a each new run.
"""

import numpy as np
import os
from time import time_ns
from constants import (
    ZMQ_PORT,
    SAVE_RESULTS_AFTER_ITERATIONS,
    MAX_ITERATIONS,
    DELETE_WHEN_PROPORTION_SPACE_LEFT,
)
import zmq


def new_run(socket, h5_save_path, run_number):

    # New directory to store run.
    h5_save_path_this_run = os.path.join(h5_save_path, f"run_num_{run_number}")
    print(f"hdf5_chunk_writer: starting new run {run_number}")
    os.makedirs(h5_save_path_this_run)

    print(f"hdf5_chunk_writer: recieving chunks from server")

    # We want the server to wait for the client to request another run,
    # and it will hand on recv.
    socket.send("".encode("ascii"))

    chunks = socket.recv_multipart()
    print(f"hdf5_chunk_writer: recieved chunks from server")

    # We only want to time write, not read.
    start_time = time_ns()
    for i in range(len(chunks)):
        with open(os.path.join(h5_save_path_this_run, f"chunk_{i}.dat"), "wb") as f:
            f.write(chunks[i])
    end_time = time_ns()
    return end_time - start_time


def save_times(times_array, times_file_path):
    np.save(times_file_path, times_array)


def get_zmq_client_socket(port=ZMQ_PORT):
    print("hdf5_chunk_writer: connecting to zmq server")
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://127.0.0.1:{port}")
    return socket


def retrieve_chunks_and_save_to_shm(
    port,
    h5_save_path,
    times_path,
    save_after_iterations=SAVE_RESULTS_AFTER_ITERATIONS,
    max_iterations=MAX_ITERATIONS,
):
    socket = get_zmq_client_socket(port=port)

    times_array = np.empty(max_iterations)
    times_array.fill(-1)

    for iteration in range(max_iterations):
        time_taken = new_run(socket, h5_save_path, iteration)

        times_array[iteration] = time_taken

        if (iteration + 1) % save_after_iterations == 0:
            print(
                f"time array: another {save_after_iterations} runs complete, saving results to {times_path}"
            )
            save_times(times_array, times_path)


class CircularBuffer:
    def __init__(self, file_dir, file_name):
        self.file_path = os.path.join(file_dir, file_name)
        self.file_dir = file_dir

        # Number of bytes into the file the last written chunk ends.
        self.current_chunk_end_bytes_in = 0

    def write_empty_file(self):
        sizes = os.statvfs(self.file_dir)
        self.file_size_bytes = int(
            (1 - DELETE_WHEN_PROPORTION_SPACE_LEFT) * (sizes.f_bfree * sizes.f_frsize)
        )
        print(f"FILE SIZE BYTES: {self.file_size_bytes}")

        with open(self.file_path, "wb+") as file:
            file.write(bytes(self.file_size_bytes))

    def write_chunks(self, socket):

        socket.send("".encode("ascii"))

        chunks = socket.recv_multipart()
        print(f"hdf5_chunk_writer: recieved chunks from server")

        start_time = time_ns()
        with open(self.file_path, "rb+") as file:

            for chunk in chunks:
                chunk_bytes = len(chunk)
                if self.current_chunk_end_bytes_in + chunk_bytes > self.file_size_bytes:
                    self.current_chunk_end_bytes_in = 0

                file.seek(self.current_chunk_end_bytes_in)
                file.write(chunk)
                self.current_chunk_end_bytes_in += chunk_bytes

        end_time = time_ns()

        return end_time - start_time


def retrieve_chunks_and_save_to_shm_circular_buffer(
    port,
    h5_save_path,
    dat_file_name,
    times_path,
    save_after_iterations=SAVE_RESULTS_AFTER_ITERATIONS,
    max_iterations=MAX_ITERATIONS,
):
    socket = get_zmq_client_socket(port=port)

    times_array = np.empty(max_iterations)
    times_array.fill(-1)

    circular_buffer = CircularBuffer(h5_save_path, dat_file_name)
    circular_buffer.write_empty_file()

    for iteration in range(max_iterations):
        time_taken = circular_buffer.write_chunks(socket)

        times_array[iteration] = time_taken

        if (iteration + 1) % save_after_iterations == 0:
            print(
                f"time array: another {save_after_iterations} runs complete, saving results to {times_path}"
            )
            save_times(times_array, times_path)
