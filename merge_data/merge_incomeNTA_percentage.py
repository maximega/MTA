import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_incomeNTA_percentage(dml.Algorithm):
    contributor = 'maximega_tcorc'
    reads = ['maximega_tcorc.transportation_percentages', 'maximega_tcorc.income_with_neighborhoods']
    writes = ['maximega_tcorc.income_with_NTA_with_percentages']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        repo_name = merge_incomeNTA_percentage.writes[0]
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # ----------------- Retrieve data from Mongodb -----------------
        trans_percent = repo.maximega_tcorc.transportation_percentages
        income_NTA = repo.maximega_tcorc.income_with_neighborhoods
        
        # ----------------- Merge Census Tract incomes with NTA info, aggregate and average incomes for NTA -----------------
        insert_many_arr = []
        count_tracts = 0
        total_income = 0
        pub_trans_percent = 0
        for nta in income_NTA.find():
            for percent in trans_percent.find():
                if nta['ntacode'] == percent['nta']:
                    pub_trans_percent = percent['percent']
                    break
            insert_many_arr.append({
                'ntacode': nta['ntacode'], 
                'ntaname': nta['ntaname'],  
                'stations': nta['stations'], 
                'population': nta['population'],
                'income': nta['income'],
                'position': nta['position'],
                'geom' : nta['geom'],
                'trans_percent': pub_trans_percent
            })

        #----------------- Data insertion into Mongodb ------------------
        repo.dropCollection('income_with_NTA_with_percentages')
        repo.createCollection('income_with_NTA_with_percentages')
        repo[repo_name].insert_many(insert_many_arr)
        repo[repo_name].metadata({'complete':True})
        print(repo[repo_name].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return None