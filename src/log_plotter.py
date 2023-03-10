"""
Takes the various numpy arrays corresponding to length of time taken for run at run i from out/i.npy, combines them all into the same array,
then plots the values in numpy.
"""

import matplotlib.pyplot as plt
from multi_process.constants import TIME_ARRAY_OUT_ROOT
import numpy as np
import os
import sys

if __name__ == "__main__":
    if len(sys.argv) == 1:
        time_dir = TIME_ARRAY_OUT_ROOT
    else:
        time_dir = sys.argv[1]

    try:
        files = os.listdir(time_dir)
        files = [os.path.join(time_dir, file) for file in files]
    except NotADirectoryError:
        files = [time_dir]  # Passed in the file directly, instead of the directory.

    for file in files:
        if file[-4:] == ".npy":
            times = np.load(file)
            times = np.delete(times, np.where(times == -1))
            times *= 1e-9
            plt.plot(times)

    plt.show()
