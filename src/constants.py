"""For storing paths and stuff :)"""

# Path to save the numpy array for the times of runs.
TIME_ARRAY_OUT_DIRECTORY = "out"

# H5 file path. The code will take all .h5 files in this path and iterate through them.
H5_FILE_DIRECTORY = "/dls/i04-1/data/2021/cm28181-3/gw/20210720/TestInsulin/Insulin_12/"

# The code will output chunks here
H5_CHUNK_OUT_DIRECTORY = "/dev/shm"

# If a chunk index contains these as a factor then the inotify thread/core will delete it.
FACTORS_OF_CHUNKS_TO_DELETE = [3]
