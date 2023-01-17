"""
Finds most recent /dev/shm/run_num_x (highest x), then sequentially deletes .dat files corrsponding to chunks, once it's finished
waits for a new run to be created and does the same.
"""
import inotify.adapters
import os
from constants import (
    FACTORS_OF_CHUNKS_TO_DELETE,
    DELETE_WHEN_PROPORTION_SPACE_LEFT,
    NUMBER_OF_RUNS_TO_DELETE,
    CHUNK_OUT_ROOT,
)


def delete_older_runs_if_neccessary(
    h5_output_directory,
    delete_when_proportion_space_left=DELETE_WHEN_PROPORTION_SPACE_LEFT,
):
    sizes = os.statvfs(h5_output_directory)
    dir_contents = sorted(
        [
            os.path.join(h5_output_directory, file)
            for file in os.listdir(h5_output_directory)
        ],
        key=os.path.getmtime,
    )
    if sizes.f_bfree < sizes.f_blocks * delete_when_proportion_space_left:
        print(f"inotify_chunk_deleter: deleting runs: ", end="")
        for run_to_delete in dir_contents[:NUMBER_OF_RUNS_TO_DELETE]:
            if run_to_delete[-3:] != "npy":
                print(str(run_to_delete), end=" ")
                os.system(f"rm -rf {os.path.join(h5_output_directory, run_to_delete)}")
        print("")

        # shutil rm was far too slow, must be a weird thing implementation....
        # shutil.rmtree(os.path.join(h5_output_directory, run_to_delete))


def delete_chunks(
    run_directory, factors_of_chunks_to_delete=FACTORS_OF_CHUNKS_TO_DELETE
):
    directory_files = os.listdir(run_directory)
    for i in range(len(directory_files)):
        for factor in factors_of_chunks_to_delete:
            if i % factor == 0:
                os.remove(os.path.join(run_directory, directory_files[i]))
                break


def delete_old_chunks_on_new_dir_creation(directory=CHUNK_OUT_ROOT):
    path = get_path_on_chunk_creation(directory)
    i = inotify.adapters.Inotify()
    i.add_watch(path)

    run_dir_name = None

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        if "IN_CREATE" in type_names:
            if filename[-4:] != ".dat":
                print(f"inotify_chunk_deleter: new run created {filename}")
                if run_dir_name is not None:
                    print(
                        f"inotify_chunk_deleter: deleting chunks from last run {run_dir_name}"
                    )
                    delete_chunks(os.path.join(path, run_dir_name))
                run_dir_name = filename
                delete_older_runs_if_neccessary(path)


def get_path_on_chunk_creation(directory):
    dir_contents = [run for run in os.listdir(directory) if run[:3] == "run"]
    dir_contents = sorted(
        [os.path.join(directory, file) for file in dir_contents],
        key=os.path.getmtime,
    )
    return dir_contents[-1]
