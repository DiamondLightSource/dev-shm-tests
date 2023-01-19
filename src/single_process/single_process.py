"""
To narrow down some strange behaviour observed, this code will test without using 0mq (which could be effecting write speeds when using localhost).

We only need one core for everything if we're using a circular buffer and get the chunks before we write to /dev/shm.
"""

import os
import h5py
from time import time_ns
import numpy as np
import itertools
from datetime import datetime


H5_FILE_DIRECTORY = "/dls/i04-1/data/2021/cm28181-3/gw/20210720/TestInsulin/Insulin_12/"
MAX_ARRAY_NUMBER = 4
CHUNK_OUT_ROOT = "/dev/shm"
DELETE_WHEN_PROPORTION_SPACE_LEFT = 0.1
MAX_ITERATIONS = 1000000
SAVE_RESULTS_AFTER_ITERATIONS = 100
RESULTS_ARRAY_OUT_ROOT = "out"


def get_file_paths(path):
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


def load_images(images_directory=H5_FILE_DIRECTORY, max_array_number=4):
    # Returns and array of chunk arrays: [[file_1_chunk_1, file_chunk_2, ...], [file_2_chunk_1, file_2_chunk_2, ...], ...].
    chunks_list = []

    file_paths = get_file_paths(images_directory)
    for file_path in file_paths:
        with h5py.File(file_path) as h5_file:
            data = h5_file["data"]
            chunks = []

            # Read chunks into memory.
            for chunk_num in range(data.shape[0]):
                chunks.append(data.id.read_direct_chunk((chunk_num, 0, 0))[1])

            print(f"loaded file {file_path} containing {len(chunks)} chunks")
            chunks_list.append(chunks)

            if max_array_number:
                if len(chunks_list) >= max_array_number:
                    break

    return chunks_list


def write_empty_file(file_name, root_dir=CHUNK_OUT_ROOT):
    buffer_file_path = os.path.join(root_dir, file_name)
    sizes = os.statvfs(root_dir)
    buffer_file_size = int(
        (1 - DELETE_WHEN_PROPORTION_SPACE_LEFT) * (sizes.f_bfree * sizes.f_frsize)
    )
    with open(buffer_file_path, "wb+") as file:
        file.write(bytes(buffer_file_size))

    return buffer_file_path, buffer_file_size


def generate_results_array_and_path(
    file_name,
    max_iterations=MAX_ITERATIONS,
    results_array_out_root=RESULTS_ARRAY_OUT_ROOT,
):
    results_array = np.empty(max_iterations)
    results_array.fill(-1)
    return results_array, os.path.join(results_array_out_root, file_name)


def write_chunks(
    file,
    chunks,
    buffer_file_size,
    current_bytes_in,
):
    start_time = time_ns()

    for chunk in chunks:
        chunk_bytes = len(chunk)
        if current_bytes_in + chunk_bytes > buffer_file_size:
            current_bytes_in = 0

        file.seek(current_bytes_in)
        file.write(chunk)
        current_bytes_in += chunk_bytes

    end_time = time_ns()

    return end_time - start_time, current_bytes_in


def save_results(results_array, results_array_path):
    np.save(results_array_path, results_array)


def write_chunks_loop(
    chunks_list,
    results_array,
    results_array_path,
    buffer_file_path,
    buffer_file_size,
    max_iterations=MAX_ITERATIONS,
    save_results_after_iterations=SAVE_RESULTS_AFTER_ITERATIONS,
):

    current_bytes_in = 0
    with open(buffer_file_path, "rb+") as file:
        iteration = 0
        print("", end="\r")
        for (iteration, chunks) in zip(
            range(max_iterations), itertools.cycle(chunks_list)
        ):
            results_array[iteration], current_bytes_in = write_chunks(
                file, chunks, buffer_file_size, current_bytes_in
            )
            print(
                f"\033[Kiteration {iteration} took {round(results_array[iteration]*1e-9, 4)} seconds",
                end="\r",
            )
            if (iteration) % save_results_after_iterations == 0:
                save_results(results_array, results_array_path)


def main():

    start_time_str = datetime.now().strftime("%m-%d-%H-%M-%S")
    print("loading chunk list")
    chunks_list = load_images()
    print("finished loading chunk list")
    results_array, results_array_path = generate_results_array_and_path(
        start_time_str + ".npy"
    )
    print(f"results array will be saved to {results_array_path}")
    print("generating empty file")
    buffer_file_path, buffer_file_size = write_empty_file(start_time_str + ".dat")
    print(
        f"finished generating empty dat file {buffer_file_path}, of size {round(buffer_file_size  / (1024 ** 3), 4)} GB"
    )

    print("starting chunk writing loop")
    write_chunks_loop(
        chunks_list,
        results_array,
        results_array_path,
        buffer_file_path,
        buffer_file_size,
    )


if __name__ == "__main__":
    main()
