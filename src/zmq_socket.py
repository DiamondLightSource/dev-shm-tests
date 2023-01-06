"""
Loads h5 files into memory and stores them as arrays of chunks.
Launches a 0mq socket on loopback so that hdf5_chunk_writer can recieve chunks faster than writing to disk.
"""

from constants import H5_FILE_DIRECTORY
import os
import h5py
import zmq
from constants import ZMQ_PORT
import itertools
import pickle

def get_h5_file_paths(path=H5_FILE_DIRECTORY):
    path_filenames = os.listdir(path)
    h5_file_paths = [os.path.join(path, filename) for filename in path_filenames if filename[-3:] == ".h5"] 
    
    # Check to see if all the hdf5 files have data and don't use the ones which don't in out list.
    for h5_file_path in h5_file_paths:
        try:
            f = h5py.File(h5_file_path, "r")
            f["data"]
        except KeyError: # we want to skip over files without data
            del h5_file_paths[h5_file_paths.index(h5_file_path)]

    return h5_file_paths

def load_h5_files_as_arrays_of_chunks(h5_file_paths, max_array_number=None, pickled=False):
    chunks_arrays = []

    for h5_file_path in h5_file_paths:
        print("zmq_socket: loading chunks from " + h5_file_path)
        with h5py.File(h5_file_path) as h5_file:
            data = h5_file["data"]
            chunks = []

            # Read chunks into memory.
            for chunk_num in range(data.shape[0]):
                chunks.append(data.id.read_direct_chunk((chunk_num, 0, 0))[1])

            if pickled:
                chunks = pickle.dumps(chunks)

            chunks_arrays.append(chunks)

            if max_array_number is not None:
                if len(chunks_arrays) >= max_array_number:
                    break


    return chunks_arrays


def get_zmq_server_socket(port=ZMQ_PORT):
    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://127.0.0.1:{port}")
    print("zmq_socket: starting server")
    return socket


def start_zmp_chunk_server(port=ZMQ_PORT):
    chunk_arrays = load_h5_files_as_arrays_of_chunks(get_h5_file_paths(), max_array_number=3, pickled=True)
    socket = get_zmq_server_socket(port=port)

    for chunk_array_pickled in itertools.cycle(chunk_arrays):
        socket.recv()
        print("zmq_socket: sending chunk")
        socket.send(chunk_array_pickled)

if __name__ == "__main__":
    start_zmp_chunk_server()