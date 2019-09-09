import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from merge_data.helper_functions.within_polygon import point_inside_polygon


class merge_stations_nta(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.stations', 'maximega_tcorc.neighborhoods']
	writes = ['maximega_tcorc.neighborhoods_with_stations']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		repo_name = merge_stations_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		stations = repo.maximega_tcorc.stations.find()
		ntas = repo.maximega_tcorc.neighborhoods.find()

		duplicates = []
		filtered_stations = []
		# ----------------- Create a list of unique stations (stations data contains many duplicates) -----------------
		for station in stations:
			lat = station['Station Latitude']
			lon = station['Station Longitude']
			if (lat, lon) not in duplicates:
				duplicates.append((lat, lon))
				filtered_stations.append(station)

		nta_objects = {}
		nta_count = 0
		station_count = 0
		# ----------------- Merge NTA info with station info if the subway station is inside of NTA multi polygon -----------------
		for nta in ntas:
			nta_objects[nta['ntacode']] = {'ntacode': nta['ntacode'],'ntaname': nta['ntaname'], 'the_geom': nta['the_geom'], 'stations': []}
			nta_multipolygon = []
			min_lat = 0
			max_long = 0
			for i in nta['the_geom']['coordinates']:
				for j in i:
					for k in range(len(j)):
						nta_multipolygon.append(j[k])
			for coord in nta_multipolygon:
				neg_lat = coord[0]
				pos_long = abs(coord[1])
				if neg_lat < min_lat:
					min_lat = neg_lat
				if pos_long > max_long:
					max_long = pos_long

			nta_objects[nta['ntacode']]['position'] = [min_lat, max_long]

			for station in filtered_stations:
				# ----------------- station coordinates come in form: (lat, long) as a string -----------------
				# ----------------- retreive lat and long points, cast them to floats to be passed into point_inside_polygon function -----------------				
				station_coordinates = station['Station Location']
				lat_coord = ''
				long_coord = ''
				i = 1
				while(station_coordinates[i] != ','):
					lat_coord += station_coordinates[i]
					i += 1
				i += 2
				while(station_coordinates[i] != ')'):
					long_coord += station_coordinates[i]
					i += 1
				lat_coord = float(lat_coord)
				long_coord = float(long_coord)
				is_in_nta = point_inside_polygon(long_coord, lat_coord, nta_multipolygon)
				if is_in_nta:
					nta_objects[nta['ntacode']]['stations'].append({
						'station_name': station['Station Name']
					})

		

		# ----------------- Reformat data for mongodb insertion -----------------
		insert_many_arr = []
		for key in nta_objects.keys():
			insert_many_arr.append(nta_objects[key])

		#----------------- Data insertion into Mongodb ------------------
		repo.dropCollection('neighborhoods_with_stations')
		repo.createCollection('neighborhoods_with_stations')
		repo[repo_name].insert_many(insert_many_arr)
		repo[repo_name].metadata({'complete':True})
		print(repo[repo_name].metadata())

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return None
