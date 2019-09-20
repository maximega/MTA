import json
import pymongo
import urllib.request
import uuid

class get_neighborhoods():
    reads = []
    writes = ['mta.neighborhoods']

    @staticmethod
    def execute(trial = False):
        repo_name = get_neighborhoods.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo
        
        #------------------ Data retrieval ---------------------
        url = 'https://data.cityofnewyork.us/resource/q2z5-ai38.json'
        request = urllib.request.Request(url)
        response = urllib.request.urlopen(request)
        content = response.read()
        json_response = json.loads(content)
        json_string = json.dumps(json_response, sort_keys=True, indent=2)

        #----------------- Data insertion into Mongodb ------------------
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(json_response)
        
        repo.logout()
        print(repo_name, "completed")