import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas
from pandas.plotting import parallel_coordinates

from maximega_tcorc_goferbat.helper_functions.lat_long_kmeans import run_lat_long_kmeans
from maximega_tcorc_goferbat.helper_functions.cons_sat import cons_sat
from maximega_tcorc_goferbat.helper_functions.Correlation import Correlation

# from helper_functions.lat_long_kmeans import run_lat_long_kmeans
# from helper_functions.cons_sat import cons_sat
# from helper_functions.Correlation import Correlation


class kmeans_opt(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.income_with_NTA_with_percentages']
	writes = ['maximega_tcorc.new_zone_fares']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		repo_name = kmeans_opt.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		# -----------------Retrieve neighborhood data from Mongodb -----------------
		nta_objects = repo.maximega_tcorc.income_with_NTA_with_percentages.find()

		# ----------------- If the trial flag is set, only use a small sample of the data -----------------
		if trial:
			nta_objects = nta_objects[0:50]

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

		# ----------------- Finding the correlation between avergae income and % of popoulaiton using public transport-----------------
		Correlation(incomes, pops)

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
			repo.dropCollection('new_zone_fares')
			repo.createCollection('new_zone_fares')
			repo[repo_name].insert_many(data_copy)
			repo[repo_name].metadata({'complete':True})
			print(repo[repo_name].metadata())

			repo.logout()

		if (new_fares == 'unsat'):
			print("Constraints Were Not Satisfied")
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		#agent
		this_script = doc.agent('alg:maximega_tcorc#kmeans_opt', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		#resource
		nta_objects = doc.entity('dat:maximega_tcorc#income_with_NTA_with_percentages', {prov.model.PROV_LABEL:'Income with NTA with Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
		#agent
		running_k_means_cons_sat = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(running_k_means_cons_sat, this_script)

		doc.usage(running_k_means_cons_sat, nta_objects, startTime, None,
					{prov.model.PROV_TYPE:'ont:Computation'
					}
					)
		#reasource
		categorized_ntas = doc.entity('dat:maximega_tcorc#categorized_ntas', {prov.model.PROV_LABEL:'Categorized NTAS', prov.model.PROV_TYPE:'ont:DataSet'})
		
		doc.wasAttributedTo(categorized_ntas, this_script)
		doc.wasGeneratedBy(categorized_ntas, running_k_means_cons_sat, endTime)
		doc.wasDerivedFrom(categorized_ntas, nta_objects, running_k_means_cons_sat, running_k_means_cons_sat, running_k_means_cons_sat)

		repo.logout()
				
		return doc

kmeans_opt.execute()
