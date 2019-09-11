import datetime
import json
import pymongo
import urllib.request
import uuid

class merge_income():
	reads = ['mta.income_with_tracts', 'mta.population_with_neighborhoods']
	writes = ['mta.income_with_neighborhoods']
	
	@staticmethod
	def execute():
		startTime = datetime.datetime.now()

		repo_name = merge_income.writes[0]
		# ----------------- Set up the database connection -----------------
		client = pymongo.MongoClient()
		repo = client.repo

        # ----------------- Retrieve data from Mongodb -----------------
		incomes = repo.mta.income_with_tracts
		neighborhoods = repo.mta.population_with_neighborhoods
		
		# ----------------- Merge Census Tract incomes with NTA info, aggregate and average incomes for NTA -----------------
		insert_many_arr = []
		for neighborhood in neighborhoods.find():
			count_tracts = 0
			total_income = 0
			# ----------------- Exclude all tracts that have 0 population (airports, parks, prisons, cemetaries...) -----------------
			if (neighborhood['ntaname'] != 'Airport' and 'park' not in neighborhood['ntaname'] and neighborhood['ntaname'] != 'Rikers Island'):
				for income in incomes.find():
					if neighborhood['ntacode'] == income['nta']:
						count_tracts += 1
						total_income += float(income['income'])
				avg_income = total_income/count_tracts
				insert_many_arr.append({
				'ntacode': neighborhood['ntacode'], 
				'ntaname': neighborhood['ntaname'],  
				'stations': neighborhood['stations'], 
				'population': neighborhood['population'],
				'position' : neighborhood['position'],
				'geom' : neighborhood['the_geom'],
				'income': avg_income
				})

		#----------------- Data insertion into Mongodb ------------------
		repo.drop_collection(repo_name)
		repo.create_collection(repo_name)
		repo[repo_name].insert_many(insert_many_arr)

		repo.logout()

		endTime = datetime.datetime.now()

		print(repo_name, "completed")