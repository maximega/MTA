import sys
import os
from os import path
import importlib
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("contributor_folder")
args = parser.parse_args()
data_set_file = "get_data_write.txt"

# Extract the algorithm classes from the modules in the
# subdirectory specified on the command line.
subdir = args.contributor_folder
algorithms = []
for r,d,f in os.walk(subdir):
    for file in f:
        if r.find(os.sep) == -1 and file.split(".")[-1] == "py":
            name_module = ".".join(file.split(".")[0:-1])
            print(subdir + "." + name_module)
            module = importlib.import_module(subdir + "." + name_module)
            algorithms.append(module.__dict__[name_module])

# Create an ordering of the algorithms based on the data
# sets that they read and write.
datasets = set()
if subdir == 'merge_data':
    if path.exists(data_set_file):
        datasets = set(line.strip() for line in open(data_set_file))
        os.remove(data_set_file)
    else:
        print()
        print("Please execute 'get_data' subdirectory before executing 'merge_data' as data must be retrieved before it can be utilized")
        print("To do this please enter 'python run.py get_data' into the command line")
        print()
        exit()

ordered = []
print()
while len(algorithms) > 0:
    for i in range(0,len(algorithms)):
        if set(algorithms[i].reads).issubset(datasets):
            datasets = datasets | set(algorithms[i].writes)
            ordered.append(algorithms[i])
            del algorithms[i]
            break

if subdir == 'get_data':
    f = open(data_set_file,"w+")
    for item in datasets:
        f.write("%s\r\n" % (item))
    f.close()
            
# Execute the algorithms in order.
for algorithm in ordered:
    algorithm.execute()