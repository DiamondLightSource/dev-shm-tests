from mpi4py import MPI
import zmq
import pickle
import numpy as np
import time
import logging

PORT = 5556
DATA_TO_SEND = pickle.dumps(np.random.rand(1000000))

def run_server():
    socket = get_zmq_server_socket(PORT)
    for _ in range(100000000):
        socket.recv()
        socket.send(DATA_TO_SEND)
        time.sleep(0.0001)


def get_zmq_server_socket(port):
    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://127.0.0.1:{port}")
    logging.info("zmq_socket: starting server")
    return socket

def run_client():
    socket = get_zmq_client_socket(PORT)
    for i in range(1000000):
        socket.send("".encode("ascii"))
        var = pickle.loads(socket.recv())
        logging.info(var)

def get_zmq_client_socket(port):
    logging.info("hdf5_chunk_writer: connecting to zmq server")
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://127.0.0.1:{port}")
    return socket

def main():
    comm  = MPI.COMM_WORLD

    rank = comm.Get_rank()

    if rank == 0:
        run_server()

    elif rank == 1:
        run_client()


if __name__ == "__main__":
    main()