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

		# ----------------- Neighborhoods that include multiple land masses -----------------
		multi_shape = ['Pelham Bay-Country Club-City Island', 'Lenox Hill-Roosevelt Island', 'Breezy Point-Belle Harbor-Rockaway Park-Broad Channel']
		# ----------------- Neighborhoods with extra/unecessary polygon points -----------------
		polygon_errors = ['Lower East Side', 'Battery Park City-Lower Manhattan', 'Murray Hill-Kips Bay', 'Mott Haven-Port Morris', 'East Harlem North', 'Yorkville']
		# ----------------- Q train extensions made in 2017 -----------------
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
		# ----------------- Extract the locations of Q-train 2017 stop extensions and add to 2015 stations db -----------------
		for stop in q_train:
			if stop['NAME'] in extensions:
				stop['Station Name'] = stop['NAME']
				temp = stop['the_geom'][6:]
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
				stop['Station Latitude'] = float(long_coord)
				stop['Station Longitude'] = float(lat_coord)
				stop['Station Location'] = "(" + long_coord + ", " + lat_coord + ")"
				filtered_stations.append(stop)

		# ----------------- Merge NTA info with station info if the subway station is inside of NTA multi polygon -----------------
		for nta in ntas:
			nta_objects[nta['ntacode']] = {'ntacode': nta['ntacode'],'ntaname': nta['ntaname'], 'stations': []}
			nta_multipolygon = []
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
			elif nta['ntaname'] not in multi_shape:
				for i in nta['the_geom']['coordinates']:
					for j in i:
						for k in range(len(j)):
							nta_multipolygon.append(j[k])
				nta_objects[nta['ntacode']]['the_geom'] = nta_multipolygon

			total_long = 0
			total_lat = 0
			count_points = 0
			if nta['ntaname'] in multi_shape:
				for i in nta['the_geom']['coordinates']:
					for j in i:
						for k in range(len(j)):
							total_lat += j[k][0]
							total_long += j[k][1]
							count_points += 1
			else:
				for coord in nta_multipolygon:
					total_lat += coord[0]
					total_long += coord[1]
					count_points +=1

			nta_objects[nta['ntacode']]['position'] = [total_long/count_points, total_lat/count_points]

			for station in filtered_stations:
				lat_coord = station['Station Latitude']
				long_coord = station['Station Longitude']
				if nta['ntaname'] not in multi_shape:
					is_in_nta = point_inside_polygon(long_coord, lat_coord, nta_multipolygon)
					if is_in_nta:
						nta_objects[nta['ntacode']]['stations'].append({
							'station_name': station['Station Name']
						})
				else:
					nta_objects[nta['ntacode']]['the_geom'] = nta['the_geom']['coordinates']
					for i in nta['the_geom']['coordinates']:
						for j in i:
							is_in_nta = point_inside_polygon(long_coord, lat_coord, j)
							if is_in_nta:
								nta_objects[nta['ntacode']]['stations'].append({
									'station_name': station['Station Name']
								})
								break
						if is_in_nta:
							break
		

		# ----------------- Reformat data for mongodb insertion -----------------
		insert_many_arr = []
		for key in nta_objects.keys():
			insert_many_arr.append(nta_objects[key])
		
		# for i in insert_many_arr:
		# 	if len(i['stations']) == 0 and 'MN' in i['ntacode']:
		# 		print(i['ntaname'])
		# 		print()
		# 	if i['ntaname'] in polygon_errors:
		# 		print(i['ntaname'])
		# 		print(i['stations'])
		# 		print()
			
		#----------------- Data insertion into Mongodb ------------------
		repo.drop_collection(repo_name)
		repo.create_collection(repo_name)
		repo[repo_name].insert_many(insert_many_arr)

		repo.logout()
		print(repo_name, "completed")