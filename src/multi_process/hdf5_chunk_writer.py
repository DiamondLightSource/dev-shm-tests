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
    CHUNK_OUT_ROOT,
    TIME_ARRAY_OUT_ROOT,
)
import zmq


class Buffer:
    def __init__(
        self,
        port=ZMQ_PORT,
        max_iterations=MAX_ITERATIONS,
        save_times_after=SAVE_RESULTS_AFTER_ITERATIONS,
        chunk_out_root=CHUNK_OUT_ROOT,
        time_array_out_root=TIME_ARRAY_OUT_ROOT,
    ):
        self.start_time = time_ns()
        self.socket = self.get_zmq_client_socket(port=port)
        self.max_iterations = max_iterations
        self.save_times_after = save_times_after
        self.times_array = np.empty(max_iterations)
        self.times_array.fill(-1)
        self.times_array_path = os.path.join(
            time_array_out_root, str(self.start_time) + ".npy"
        )

        self.chunk_out_root = chunk_out_root

    def get_zmq_client_socket(self, port=ZMQ_PORT):
        print("hdf5_chunk_writer: connecting to zmq server")
        context = zmq.Context.instance()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://127.0.0.1:{port}")
        return socket

    def save_times(self):
        np.save(self.times_array_path, self.times_array)


class DirectoryBuffer(Buffer):
    def __init__(self):
        Buffer.__init__(self)
        self.run_path = self.make_new_run_directory()

    def make_new_run_directory(self):
        run_number = self.get_new_run_number()
        run_path = os.path.join(self.chunk_out_root, "run_num_" + str(run_number))
        try:
            os.mkdir(run_path)
        except FileExistsError:
            ...
        return run_path

    def get_new_run_number(self):
        dir_contents = [
            run for run in os.listdir(self.chunk_out_root) if run[:3] == "run"
        ]
        dir_contents = sorted(
            [os.path.join(self.chunk_out_root, file) for file in dir_contents],
            key=os.path.getmtime,
        )

        if dir_contents:
            return int(dir_contents[-1].split("_")[-1]) + 1
        else:
            return 0

    def write_chunks(self, sub_run_path):

        # New directory to store run.
        os.makedirs(sub_run_path)

        print(f"hdf5_chunk_writer: recieving chunks from server")

        # We want the server to wait for the client to request another run,
        # and it will hand on recv.
        self.socket.send(b"")

        chunks = self.socket.recv_multipart()
        print(f"hdf5_chunk_writer: recieved chunks from server")

        # We only want to time write, not read.
        start_time = time_ns()
        for i in range(len(chunks)):
            with open(os.path.join(sub_run_path, f"chunk_{i}.dat"), "wb") as f:
                f.write(chunks[i])
        end_time = time_ns()
        return end_time - start_time

    def write_chunks_loop(self):
        for iteration in range(self.max_iterations):
            sub_run_path = os.path.join(self.run_path, "sub_run_num_" + str(iteration))

            self.times_array[iteration] = self.write_chunks(sub_run_path)

            if (iteration + 1) % self.save_times_after == 0:
                print(
                    f"time array: another {self.save_times_after} runs complete, saving results to {self.times_array_path}"
                )
                self.save_times()


class CircularBuffer(Buffer):
    def __init__(self):
        Buffer.__init__(self)

        self.buffer_file_path = os.path.join(
            self.chunk_out_root, str(self.start_time) + ".dat"
        )

        self.write_empty_file()
        self.current_chunk_end_bytes_in = 0


    def write_empty_file(self):
        sizes = os.statvfs(self.chunk_out_root)
        self.file_size_bytes = int(
            (1 - DELETE_WHEN_PROPORTION_SPACE_LEFT) * (sizes.f_bfree * sizes.f_frsize)
        )
        print(f"FILE SIZE BYTES: {self.file_size_bytes}")

        with open(self.buffer_file_path, "wb+") as file:
            file.write(bytes(self.file_size_bytes))

    def write_chunks(self):
        self.socket.send(b"")

        chunks = self.socket.recv_multipart()
        print(f"hdf5_chunk_writer: recieved chunks from server")

        start_time = time_ns()
        with open(self.buffer_file_path, "rb+") as file:

            for chunk in chunks:
                chunk_bytes = len(chunk)
                if self.current_chunk_end_bytes_in + chunk_bytes > self.file_size_bytes:
                    self.current_chunk_end_bytes_in = 0

                file.seek(self.current_chunk_end_bytes_in)
                file.write(chunk)
                self.current_chunk_end_bytes_in += chunk_bytes

        end_time = time_ns()

        return end_time - start_time

    def write_chunks_loop(self):
        for iteration in range(self.max_iterations):
            self.times_array[iteration] = self.write_chunks()

            if (iteration + 1) % self.save_times_after == 0:
                print(
                    f"time array: another {self.save_times_after} runs complete, saving results to {self.times_array_path}"
                )
                self.save_times()
