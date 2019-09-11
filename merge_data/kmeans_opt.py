import urllib.request
import json
import pymongo
import datetime
import uuid
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas
from pandas.plotting import parallel_coordinates

from merge_data.helper_functions.lat_long_kmeans import run_lat_long_kmeans
from merge_data.helper_functions.cons_sat import cons_sat



class kmeans_opt():
	reads = ['mta.income_with_NTA_with_percentages']
	writes = ['mta.new_zone_fares']
	
	@staticmethod
	def execute():
		startTime = datetime.datetime.now()

		repo_name = kmeans_opt.writes[0]
		# ----------------- Set up the database connection -----------------
		client = pymongo.MongoClient()
		repo = client.repo

		# -----------------Retrieve neighborhood data from Mongodb -----------------
		nta_objects = repo.mta.income_with_NTA_with_percentages.find()

		incomes = []
		pops = []
		X = []
		data_copy = []
		# ----------------- Create a copy of the data, only including neihgborhoods with a subway station -----------------
		for nta in nta_objects:
			if(len(nta['stations'])!= 0):
				income = nta['income']
				X.append([nta['ntaname'], nta['position'][0], nta['position'][1], income])
				data_copy.append(nta)
				incomes.append(income)
				pops.append(nta['trans_percent'])

		# ----------------- k =5 derived from error graph in kmeans file -----------------
		k = 5

		#------------------ Run k Means on ([lat, long], avg_income) -----------------
		kmeans = run_lat_long_kmeans(X, k)

		k_groupings = kmeans.labels_
		for i in range(len(data_copy)):
			data_copy[i]['zone'] = k_groupings[i]
		avg_inc = [0] * k
		count_inc = [0] * k

		# ----------------- Find and insert average income for each zone -----------------
		for item in data_copy:
			avg_inc[item['zone']] += item['income']
			count_inc[item['zone']] += 1
		for i in range(len(avg_inc)):
			avg_inc[i] /= count_inc[i]
		for i in range(len(data_copy)):
			data_copy[i]['avg_inc'] = avg_inc[data_copy[i]['zone']]
		# ----------------- Reorder the zones based on avg_zone_income (income zone1 > income zone 2 ...) -----------------
		for i in range(1, k +1):
			max_avg = max(avg_inc)
			for item in data_copy:
				if (item['avg_inc'] == max_avg):
					item['zone'] = i
			avg_inc.remove(max_avg)
		# ----------------- Use z3 to find new zone fares that satisfy constraint set -----------------	
		new_fares = cons_sat(data_copy, k)
		print(new_fares)
		# ----------------- If constraint set was not satisfied, do not insert into mongodb -----------------
		if (new_fares != 'unsat'):
			# ----------------- Insert new zone fares into Mongodb -----------------
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


			#----------------- Data insertion into Mongodb ------------------
			repo.drop_collection(repo_name)
			repo.create_collection(repo_name)
			repo[repo_name].insert_many(data_copy)

			repo.logout()

		if (new_fares == 'unsat'):
			print("Constraints Were Not Satisfied")
		endTime = datetime.datetime.now()

		print(repo_name, "completed")