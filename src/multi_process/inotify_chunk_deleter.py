"""
Finds most recent /dev/shm/run_num_x (highest x), then sequentially deletes .dat files corrsponding to chunks, once it's finished
waits for a new run to be created and does the same.
"""
import inotify.adapters
import os
from constants import (
    DELETE_WHEN_PROPORTION_SPACE_LEFT,
    NUMBER_OF_RUNS_TO_DELETE,
)


class DirectoryDeleter:
    """
    Watches a directory with inotify and deletes the oldest directories when space is limited.
    """

    def __init__(
        self,
        directory,
        delete_when_proportion_space_left=DELETE_WHEN_PROPORTION_SPACE_LEFT,
        factors_of_chunks_to_delete=[],
        number_of_directories_to_delete=NUMBER_OF_RUNS_TO_DELETE,
    ):
        self.directory = directory
        self.delete_when_proportion_space_left = delete_when_proportion_space_left
        self.factors_of_chunks_to_delete = factors_of_chunks_to_delete
        self.number_of_directories_to_delete = number_of_directories_to_delete

        self.delete_old_chunks_on_new_dir_creation()

    def delete_old_chunks_on_new_dir_creation(self):
        i = inotify.adapters.Inotify()
        i.add_watch(self.directory)

        run_dir_name = None

        for event in i.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event

            if "IN_CREATE" in type_names:
                if filename[-4:] != ".dat":
                    if run_dir_name is not None:
                        self.delete_chunks(os.path.join(path, run_dir_name))
                    run_dir_name = filename
                    self.delete_older_runs_if_neccessary()

    def delete_older_runs_if_neccessary(self):
        sizes = os.statvfs(self.directory)
        dir_contents = sorted(
            [os.path.join(self.directory, file) for file in os.listdir(self.directory)],
            key=os.path.getmtime,
        )
        if sizes.f_bfree < sizes.f_blocks * self.delete_when_proportion_space_left:
            for sub_dir_to_delete in dir_contents[
                : self.number_of_directories_to_delete
            ]:
                os.system(f"rm -rf {os.path.join(self.directory, sub_dir_to_delete)}")

            # shutil rm was far too slow, must be a weird thing implementation....
            # shutil.rmtree(os.path.join(h5_output_directory, run_to_delete))

    def delete_chunks(self, directory):
        if not self.factors_of_chunks_to_delete:
            return

        directory_files = os.listdir(directory)
        for i in range(len(directory_files)):
            for factor in self.factors_of_chunks_to_delete:
                if i % factor == 0:
                    os.remove(os.path.join(directory, directory_files[i]))
                    break
