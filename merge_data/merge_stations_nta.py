import json
import pymongo
import urllib.request
import uuid
from utils.helper_functions.within_polygon import point_inside_polygon


class merge_stations_nta():
	reads = ['mta.stations', 'mta.neighborhoods', 'mta.q_train']
	writes = ['mta.neighborhoods_with_stations']
	
	@staticmethod
	def execute(trial = False):
		repo_name = merge_stations_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = pymongo.MongoClient()
		repo = client.repo

		stations = repo.mta.stations.find()
		ntas = repo.mta.neighborhoods.find()
		q_train = repo.mta.q_train.find({"LINE" : "Q"})

		#TODO: Fix Pelham Bay-Country Club-City Island, Lenox Hill-Roosevelt Island, Breezy Point-Belle Harbor-Rockaway Park-Broad Channel multi polygon representation

		todo = ['Pelham Bay-Country Club-City Island', 'Lenox Hill-Roosevelt Island', 'Breezy Point-Belle Harbor-Rockaway Park-Broad Channel']
		polygon_errors = ['Lower East Side', 'Battery Park City-Lower Manhattan', 'Murray Hill-Kips Bay', 'Mott Haven-Port Morris', 'East Harlem North', 'Yorkville']
		extensions = ['72nd St', '86th St', '96th St']
		duplicates = []
		filtered_stations = []
		# ----------------- Create a list of unique stations (stations data contains many duplicates) -----------------
		for station in stations:
			lat = station['Station Latitude']
			lon = station['Station Longitude']
			if (lon, lat) not in duplicates:
				duplicates.append((lon, lat))
				filtered_stations.append(station)

		nta_objects = {}
		# ----------------- Extract the locations of Q-train 2017 stop extensions and add to 2015 stations db  -----------------
		for stop in q_train:
			if stop['NAME'] in extensions:
				#print(stop)
				stop['Station Name'] = stop['NAME']
				temp = stop['the_geom'][7:]
				lat_coord = ''
				long_coord = ''
				i = 1
				while(temp[i] != ' '):
					lat_coord += temp[i]
					i += 1
				i += 1
				while(temp[i] != ')'):
					long_coord += temp[i]
					i += 1
				stop['Station Latitude'] = float(lat_coord)
				stop['Station Longitude'] = float(long_coord)
				stop['Station Location'] = "(" + long_coord + ", " + lat_coord + ")"
				filtered_stations.append(stop)

		# ----------------- Merge NTA info with station info if the subway station is inside of NTA multi polygon -----------------
		for nta in ntas:
			nta_objects[nta['ntacode']] = {'ntacode': nta['ntacode'],'ntaname': nta['ntaname'], 'stations': []}
			nta_multipolygon = []
			min_lat = 0
			max_long = 0
			# ----------------- Some polygons have extra, unnecessary coords, taking the max of the list of subcoords eliminates errors -----------------
			if nta['ntaname'] in polygon_errors:
				for i in nta['the_geom']['coordinates']:
					max_num_j = 0
					for j in i:
						cur_j = len(j)
						if cur_j > max_num_j:
							max_num_j = cur_j
							max_j = j
				for k in max_j:
					nta_multipolygon.append(k)
				nta_objects[nta['ntacode']]['the_geom'] = nta_multipolygon
			elif nta['ntaname'] not in todo:
				for i in nta['the_geom']['coordinates']:
					for j in i:
						for k in range(len(j)):
							nta_multipolygon.append(j[k])
				nta_objects[nta['ntacode']]['the_geom'] = nta_multipolygon

			if nta['ntaname'] not in todo:
				for coord in nta_multipolygon:
					neg_lat = coord[0]
					pos_long = abs(coord[1])
					if neg_lat < min_lat:
						min_lat = neg_lat
					if pos_long > max_long:
						max_long = pos_long

				nta_objects[nta['ntacode']]['position'] = [max_long, min_lat]

			for station in filtered_stations:
				lat_coord = station['Station Latitude']
				long_coord = station['Station Longitude']
				if nta['ntaname'] not in todo:
					is_in_nta = point_inside_polygon(long_coord, lat_coord, nta_multipolygon)
				else:
					for i in nta['the_geom']['coordinates']:
						for j in i:
							if station['Station Name'] == 'Beach 105th St' and nta['ntacode'] == 'QN10':
								is_in_nta = point_inside_polygon(long_coord, lat_coord, j)
							is_in_nta = point_inside_polygon(long_coord, lat_coord, j)
							if station['Station Name'] == 'Beach 105th St' and is_in_nta:
								print(j)
								print()
								print(long_coord)
								print(lat_coord)
								print()
				if is_in_nta:
					nta_objects[nta['ntacode']]['stations'].append({
						'station_name': station['Station Name']
					})
					if nta['ntaname'] in todo:
						print(nta_objects[nta['ntacode']])
						print()
						print()

		

		# ----------------- Reformat data for mongodb insertion -----------------
		insert_many_arr = []
		for key in nta_objects.keys():
			insert_many_arr.append(nta_objects[key])
		
		#----------------- Data insertion into Mongodb ------------------
		repo.drop_collection(repo_name)
		repo.create_collection(repo_name)
		repo[repo_name].insert_many(insert_many_arr)

		repo.logout()
		print(repo_name, "completed")