import datetime
import json
import pymongo
import urllib.request
import uuid

class merge_population_nta():
	reads = ['mta.population', 'mta.neighborhoods_with_stations']
	writes = ['mta.population_with_neighborhoods']
	
	@staticmethod
	def execute():
		startTime = datetime.datetime.now()

		repo_name = merge_population_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = pymongo.MongoClient()
		repo = client.repo

		populations = repo.mta.population
		neighborhoods = repo.mta.neighborhoods_with_stations
		
		# ----------------- NTA info with NTA populations -----------------
		insert_many_arr = []
		for neighborhood in neighborhoods.find():
			for population in populations.find():
				if neighborhood['ntacode'] == population['nta_code']:
					insert_many_arr.append({
						'ntacode': neighborhood['ntacode'], 
						'ntaname': neighborhood['ntaname'], 
						'the_geom': neighborhood['the_geom'], 
						'stations': neighborhood['stations'], 
						'position' : neighborhood['position'],
						'population': population['population']
					})
					break

		#----------------- Data insertion into Mongodb ------------------
		repo.drop_collection(repo_name)
		repo.create_collection(repo_name)
		repo[repo_name].insert_many(insert_many_arr)

		repo.logout()

		endTime = datetime.datetime.now()

		print(repo_name, "completed")

