import json
import pandas as pd
import pymongo
import urllib.request
import uuid


class get_qtrain():
    reads = []
    writes = ['mta.q_train']

    @staticmethod
    def execute():
        repo_name = get_qtrain.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo

        # ------------------ Data retrieval ---------------------

        url = 'http://datamechanics.io/data/maximega_tcorc/q_train_ext.csv'
        data = pd.read_csv(url).to_json(orient = "records")
        json_response = json.loads(data)

        # ----------------- Data insertion into Mongodb ------------------
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(json_response)

        repo.logout()
        print(repo_name, "completed")