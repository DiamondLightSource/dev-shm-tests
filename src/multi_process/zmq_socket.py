"""
Loads h5 files into memory and stores them as arrays of chunks.
Launches a 0mq socket on loopback so that hdf5_chunk_writer can recieve chunks faster than writing to disk.
"""

from constants import MAX_NUM_H5_FILES, H5_FILE_DIRECTORY, ZMQ_PORT
import os
import h5py
import zmq
import itertools


class ZMQServer:
    def __init__(
        self,
        zmq_port=ZMQ_PORT,
        h5_file_directory=H5_FILE_DIRECTORY,
        images_to_use=MAX_NUM_H5_FILES,
    ):

        self.chunks = self.load_h5_files_as_arrays_of_chunks(
            self.get_h5_file_paths(h5_file_directory), max_array_number=images_to_use
        )

        self.socket = self.get_zmq_server_socket(zmq_port)

        self.run_chunk_server()

    def run_chunk_server(self):
        for chunk_array in itertools.cycle(self.chunks):
            self.socket.recv()
            print("zmq_socket: sending chunks")
            self.socket.send_multipart(chunk_array)

    def get_h5_file_paths(self, path):
        path_filenames = os.listdir(path)
        h5_file_paths = [
            os.path.join(path, filename)
            for filename in path_filenames
            if filename[-3:] == ".h5"
        ]

        # Check to see if all the hdf5 files have data and don't use the ones which don't in out list.
        for h5_file_path in h5_file_paths:
            try:
                f = h5py.File(h5_file_path, "r")
                f["data"]
            except KeyError:  # we want to skip over files without data
                del h5_file_paths[h5_file_paths.index(h5_file_path)]

        return h5_file_paths

    def get_zmq_server_socket(self, port):
        context = zmq.Context.instance()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://127.0.0.1:{port}")
        print("zmq_socket: starting server")
        return socket

    def load_h5_files_as_arrays_of_chunks(self, h5_file_paths, max_array_number=None):
        chunks_arrays = []

        for h5_file_path in h5_file_paths:
            print("zmq_socket: loading chunks from " + h5_file_path)
            with h5py.File(h5_file_path) as h5_file:
                data = h5_file["data"]
                chunks = []

                # Read chunks into memory.
                for chunk_num in range(data.shape[0]):
                    chunks.append(data.id.read_direct_chunk((chunk_num, 0, 0))[1])

                chunks_arrays.append(chunks)

                if max_array_number:
                    if len(chunks_arrays) >= max_array_number:
                        break

        return chunks_arrays
