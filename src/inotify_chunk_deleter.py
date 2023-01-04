"""
Finds most recent /dev/shm/run_num_x (highest x), then sequentially deletes .dat files corrsponding to chunks, once it's finished
waits for a new run to be created and does the same.
"""
import inotify.adapters
import os
from constants import FACTORS_OF_CHUNKS_TO_DELETE
import shutil


def delete_older_runs_if_neccessary(h5_output_directory):
    sizes = os.statvfs(h5_output_directory)
    dir_contents = os.listdir(h5_output_directory)
    while (sizes.f_bfree  < sizes.f_blocks*0.2):
        for run_to_delete in dir_contents[-4:]:
            if run_to_delete[-3:]  != "npy":
                print(f"inotify: deleting run {run_to_delete}")
                shutil.rmtree(os.path.join(h5_output_directory, run_to_delete))


def delete_chunks(run_directory, factors_of_chunks_to_delete=FACTORS_OF_CHUNKS_TO_DELETE):
    print(run_directory)
    directory_files = os.listdir(run_directory)
    for i in range(len(directory_files)):
        for factor in factors_of_chunks_to_delete:
            if i % factor == 0:
                os.remove(os.path.join(run_directory,directory_files[i]))
                break


def delete_old_chunks_on_new_dir_creation(path):
    os.makedirs(path)

    i = inotify.adapters.Inotify()
    i.add_watch(path)


    run_dir_name = None

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        if "IN_CREATE" in type_names:
            if  filename[-4:] != ".dat":
                print(f"inotify: new run run created {filename}")
                if run_dir_name is not None:
                    print(f"inotify: deleting chunks from last run {run_dir_name}")
                    delete_chunks(os.path.join(path, run_dir_name))
                run_dir_name = filename
                delete_older_runs_if_neccessary(path)