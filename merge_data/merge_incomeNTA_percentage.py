import json
import pymongo
import urllib.request
import uuid

class merge_incomeNTA_percentage():
    reads = ['mta.transportation_percentages', 'mta.income_with_neighborhoods']
    writes = ['mta.income_with_NTA_with_percentages']
    
    @staticmethod
    def execute():
        repo_name = merge_incomeNTA_percentage.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo

        # ----------------- Retrieve data from Mongodb -----------------
        trans_percent = repo.mta.transportation_percentages
        income_NTA = repo.mta.income_with_neighborhoods
        
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
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(insert_many_arr)

        repo.logout()
        print(repo_name, "completed")