from mpi4py import MPI
import zmq
import pickle
import numpy as np

PORT = 5556
DATA_TO_SEND = pickle.dumps(np.random.rand(1000000))

def run_server():
    socket = get_zmq_server_socket(PORT)
    for _ in range(100000000):
        socket.send(DATA_TO_SEND)


def get_zmq_server_socket(port):
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUSH)
    socket.bind(f"tcp://127.0.0.1:{port}")
    print("zmq_socket: starting server")
    return socket

def run_client():
    socket = get_zmq_client_socket(PORT)
    var = pickle.loads(socket.recv())
    print(var)

def get_zmq_client_socket(port):
    print("hdf5_chunk_writer: connecting to zmq server")
    context = zmq.Context.instance()
    socket = context.socket(zmq.PULL)
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