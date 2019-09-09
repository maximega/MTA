import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class get_census_income(dml.Algorithm):
    contributor = 'maximega_tcorc'
    reads = []
    writes = ['maximega_tcorc.census_income']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        repo_name = get_census_income.writes[0]
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')
        
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
        repo.dropCollection('census_income')
        repo.createCollection('census_income')
        repo[repo_name].insert_many(insert_many_arr)
        repo[repo_name].metadata({'complete':True})
        print(repo[repo_name].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return None



