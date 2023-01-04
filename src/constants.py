"""For storing constants and stuff."""
from time import time_ns

# Path to save the numpy array for the times of runs.
TIME_ARRAY_OUT_DIRECTORY = "out"
H5_FILE_DIRECTORY = "/dls/i04-1/data/2021/cm28181-3/gw/20210720/TestInsulin/Insulin_12/"
H5_CHUNK_OUT_DIRECTORY = "out"

# If a chunk index contains these as a factor then delete it.
FACTORS_OF_CHUNKS_TO_DELETE = [5, 7]
