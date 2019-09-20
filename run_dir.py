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

get_data_reads = set()
merge_data_reads = set(['mta.census_income', 'mta.census_tracts', 'mta.neighborhoods', 'mta.population', 'mta.stations', 'mta.transportation_percentages'])

# ----------------- Extract the algorithm classes from the modules in the specified subdirectory -----------------
subdir = args.contributor_folder
debug = args.debug
algorithms = []
for r,d,f in os.walk(subdir):
    if '__pycache__' not in r:
        for file in f:
            if os.path.isdir(r):
                if r.rfind('/') == -1:
                    subdir_temp = r
                else:
                    index = r.rfind('/') + 1
                    temp_dir = r[index:]
                    subdir_temp += '.' + temp_dir
            if file.split(".")[-1] == "py" and file != '__init__.py':
                if debug:
                    if file in debug:
                        name_module = ".".join(file.split(".")[0:-1])
                        module = importlib.import_module(subdir_temp + "." + name_module)
                        algorithms.append(module.__dict__[name_module])
                else:
                    name_module = ".".join(file.split(".")[0:-1])
                    module = importlib.import_module(subdir_temp + "." + name_module)
                    algorithms.append(module.__dict__[name_module])

# ----------------- This creates a correct ordering of execution when running algs in this exact ordering get_data -> merge_data -----------------
datasets = set()
if subdir == 'get_data':
    datasets = get_data_reads

elif subdir == 'merge_data':
    datasets = merge_data_reads

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

# ----------------- Executes the programs based on the above ordering -----------------
for algorithm in ordered:
    algorithm.execute()
