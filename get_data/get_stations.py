import datetime
import json
import pandas as pd
import pymongo
import urllib.request
import uuid

class get_stations():
    reads = []
    writes = ['mta.stations']

    @staticmethod
    def execute():
        startTime = datetime.datetime.now()

        repo_name = get_stations.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo

        # ------------------ Data retrieval ---------------------
        url = 'http://datamechanics.io/data/maximega_tcorc/NYC_subway_exit_entrance.csv'
        data = pd.read_csv(url).to_json(orient = "records")
        json_response = json.loads(data)


        # ----------------- Data insertion into Mongodb ------------------
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(json_response)
        
        repo.logout()

        endTime = datetime.datetime.now()

        print(repo_name, "completed")