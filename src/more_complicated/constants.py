"""For storing paths and stuff :)"""

# Rather than saving chunks to new directories, save them all to the same file, overwriting when necessary.
USE_CIRCULAR_BUFFER = False

# Path to save the numpy array for the times of runs.
TIME_ARRAY_OUT_ROOT = "out"

SAVE_RESULTS_AFTER_ITERATIONS = 100

MAX_ITERATIONS = 10000000

# H5 file path. The code will take all .h5 files in this path and iterate through them.
H5_FILE_DIRECTORY = "/dls/i04-1/data/2021/cm28181-3/gw/20210720/TestInsulin/Insulin_12/"

# The code will output chunks here
CHUNK_OUT_ROOT = "/dev/shm"

# If a chunk index contains these as a factor then the inotify thread/core will delete it.
FACTORS_OF_CHUNKS_TO_DELETE = [3, 5]

# 0mq server socket port
ZMQ_PORT = 5556

# Delete previous runs when the proprtion of space left is
DELETE_WHEN_PROPORTION_SPACE_LEFT = 0.1

# Number of runs to delete when capacity has been reached
NUMBER_OF_RUNS_TO_DELETE = 2

# The number of h5 files to cycle through
MAX_NUM_H5_FILES = 4
