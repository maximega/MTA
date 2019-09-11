import sys
import os
from os import path
import importlib
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("contributor_folder")
parser.add_argument("-d", "--debug", nargs='+', help="allow user run only specified files")
args = parser.parse_args()
data_set_file = "get_data_write.txt"

# ----------------- Extract the algorithm classes from the modules in the specified subdirectory -----------------
subdir = args.contributor_folder
debug = args.debug
algorithms = []
for r,d,f in os.walk(subdir):
    for file in f:
        if r.find(os.sep) == -1 and file.split(".")[-1] == "py":
            if debug:
                if file in debug:
                    name_module = ".".join(file.split(".")[0:-1])
                    module = importlib.import_module(subdir + "." + name_module)
                    algorithms.append(module.__dict__[name_module])
            else:
                name_module = ".".join(file.split(".")[0:-1])
                module = importlib.import_module(subdir + "." + name_module)
                algorithms.append(module.__dict__[name_module])

# ----------------- Merge_data will use the info found in data_set_file to create its ordering -----------------
# ----------------- Data_set_file was populated on a previous run by 'get_data' -----------------
datasets = set()
if subdir == 'merge_data':
    if path.exists(data_set_file):
        datasets = set(line.strip() for line in open(data_set_file))
    else:
        print()
        print("Please execute 'get_data' subdirectory before executing 'merge_data' as data must be retrieved before it can be utilized")
        print("To do this please enter 'python run.py get_data' into the command line")
        print()
        exit()

ordered = []
print()
# ----------------- Running using the -d flag gives the user full control, does not create a specific ordering of algorithms -----------------
if (debug): 
    for i in algorithms:
        ordered.append(i)
# ----------------- Create an ordering of the algorithms based on the data sets that they read and write -----------------
else:
    while len(algorithms) > 0:
        for i in range(0,len(algorithms)):
            if set(algorithms[i].reads).issubset(datasets):
                datasets = datasets | set(algorithms[i].writes)
                ordered.append(algorithms[i])
                del algorithms[i]
                break
# ----------------- When running get_data, create and populate a file that allows the prog to see what databases get_data has written to -----------------
# ----------------- This creates a correct ordering of execution when running merge_data -----------------
if subdir == 'get_data':
    f = open(data_set_file,"w+")
    for item in datasets:
        f.write("%s\r\n" % (item))
    f.close()

# ----------------- Executes the programs based on the above ordering -----------------
for algorithm in ordered:
    print(algorithm)
    algorithm.execute()
