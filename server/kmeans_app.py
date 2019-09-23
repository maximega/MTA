import datetime
import os
import pymongo
import sys
import time
import z3

root_dir = os.path.join(os.getcwd(), '..')
sys.path.append(root_dir)

from utils.helper_functions.lat_long_kmeans import run_lat_long_kmeans
from utils.helper_functions.cons_sat import cons_sat

def execute(zones, percent_max, percent_min, factor):
    reads = ['mta.income_with_NTA_with_percentages']
    writes = ['mta.new_zone_fares']
    # ----------------- A k of 5 (5 zones) derived from error graph in kmeans file -----------------
    repo_name = writes[0]
    # ----------------- Set up the database connection -----------------
    client = pymongo.MongoClient()
    repo = client.repo

    # -----------------Retrieve neighborhood data from Mongodb -----------------
    nta_objects = repo.mta.income_with_NTA_with_percentages.find()

    incomes = []
    pops = []
    X = []
    data_copy = []
    data_insert = []
    # ----------------- Create a copy of the data, only including neihgborhoods with a subway station -----------------
    for nta in nta_objects:
        if(len(nta['stations'])!= 0):
            income = nta['income']
            X.append([nta['ntaname'], nta['position'][0], nta['position'][1], income])
            data_copy.append(nta)
            incomes.append(income)
            pops.append(nta['trans_percent'])

    #------------------ Run k Means on ([lat, long], avg_income) -----------------
    #TODO: think about including neighboring nta's and how to better use the lat and long
    kmeans = run_lat_long_kmeans(X, zones)

    k_groupings = kmeans.labels_
    for i in range(len(data_copy)):
        data_copy[i]['zone'] = k_groupings[i]
    avg_inc = [0] * zones
    count_inc = [0] * zones

    # ----------------- Find and insert average income for each zone -----------------
    for item in data_copy:
        avg_inc[item['zone']] += item['income']
        count_inc[item['zone']] += 1
    for i in range(len(avg_inc)):
        avg_inc[i] /= count_inc[i]
    for i in range(len(data_copy)):
        data_copy[i]['avg_inc'] = avg_inc[data_copy[i]['zone']]
    # ----------------- Reorder the zones based on avg_zone_income (income zone1 > income zone 2 ...) -----------------
    for i in range(1, zones +1):
        max_avg = max(avg_inc)
        for item in data_copy:
            if (item['avg_inc'] == max_avg):
                item['zone'] = i
        avg_inc.remove(max_avg)
    # ----------------- Use z3 to find new zone fares that satisfy constraint set -----------------	
    #TODO: rethink the revunue and route share portion of this z3
    new_fares = cons_sat(data_copy, zones, percent_max/100, percent_min/100, factor)

    date = datetime.date.today()
    group_id = time.time()
    # ----------------- If constraint set was not satisfied, do not insert into mongodb -----------------
    if (new_fares != 'unsat'):
        for item in data_copy:
            item['routes'] = []
            for i in range(len(new_fares)):
                key = list(new_fares[i].keys())[0]
                dest_1 = int(key[1])
                dest_2 = int(key[-1])
                if item['zone'] == dest_1 or item['zone'] == dest_2:
                    x = new_fares[i][key]
                    obj = {}
                    obj[key] = x
                    item['routes'].append(obj)
            # ----------------- Create a copy of data to insert into mongo that does not include multi polygons, because they are messy and unreadable to humans -----------------
            data_insert.append({
                'date': str(date),
                'group_id': group_id,
                'routes': item['routes'],
                'trans_percent': item['trans_percent'],
                'zone': item['zone'],
                'avg_inc': item['avg_inc'],
                'ntacode': item['ntacode'],
                'ntaname': item['ntaname'],
                'stations': item['stations']
            })
        # ----------------- Insert new zone fares into Mongodb -----------------
        if (repo_name not in repo.collection_names()):
            repo.create_collection(repo_name)
        repo[repo_name].insert_many(data_insert)
        repo.logout()
        
        # ----------------- return a version of the data with polygons, so that data can be visualized -----------------
        print(repo_name, "completed")
        return data_copy

    if (new_fares == 'unsat'):
        print("Constraints Were Not Satisfied")
        return []