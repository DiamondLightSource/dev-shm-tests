"""
Takes the various numpy arrays corresponding to length of time taken for run at run i from out/i.npy, combines them all into the same array,
then plots the values in numpy.
"""

import matplotlib.pyplot as plt
from constants import TIME_ARRAY_OUT_DIRECTORY
import numpy as np
import os

if __name__ == "__main__":
    files = os.listdir(TIME_ARRAY_OUT_DIRECTORY)
    for file in files:
        if file[-4:] == ".npy":
           times = np.load(os.path.join(TIME_ARRAY_OUT_DIRECTORY, file)) 
           times = np.delete(times, np.where(times == -1))
           times *= 1e-9
           plt.plot(times)
    
    plt.show()
