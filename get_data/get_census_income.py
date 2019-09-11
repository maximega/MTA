import datetime
import json
import pandas as pd
import prov.model
import pymongo
import urllib.request
import uuid


class get_census_income():
    reads = []
    writes = ['mta.census_income']

    @staticmethod
    def execute():
        startTime = datetime.datetime.now()

        repo_name = get_census_income.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo
        
        # ------------------ Data retrieval ---------------------
        url = 'https://api.datausa.io/api/?sort=desc&show=geo&required=income&sumlevel=tract&year=2016&where=geo%3A16000US3651000'
        request = urllib.request.Request(url)
        request.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(request)
        content = response.read()

        json_response = json.loads(content)
        json_string = json.dumps(json_response, sort_keys=True, indent=2)
        
        insert_many_arr = []
        for arr in json_response['data']:
            insert_many_arr.append({
                'year': arr[0],
                'tract': arr[1],
                'income': arr[2]
            })

        # ----------------- Data insertion into Mongodb ------------------
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(insert_many_arr)
        
        repo.logout()

        endTime = datetime.datetime.now()
        
        print(repo_name, "completed")
