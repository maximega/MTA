import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class get_census_tracts(dml.Algorithm):
    contributor = 'maximega_tcorc'
    reads = []
    writes = ['maximega_tcorc.census_tracts']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        repo_name = get_census_tracts.writes[0]
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # ------------------ Data retrieval ---------------------
        url = 'http://datamechanics.io/data/maximega_tcorc/NYC_census_tracts.csv'
        data = pd.read_csv(url).to_json(orient = "records")
        json_response = json.loads(data)

        # ----------------- Data insertion into Mongodb ------------------
        repo.dropCollection('census_tracts')
        repo.createCollection('census_tracts')
        repo[repo_name].insert_many(json_response)
        repo[repo_name].metadata({'complete':True})
        print(repo[repo_name].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return None
  