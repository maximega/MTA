import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_census_data(dml.Algorithm):
    contributor = 'maximega_tcorc'
    reads = ['maximega_tcorc.census_tracts', 'maximega_tcorc.census_income']
    writes = ['maximega_tcorc.income_with_tracts']


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        repo_name = merge_census_data.writes[0]
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # ----------------- Retrieve data from Mongodb -----------------
        incomes = repo.maximega_tcorc.census_income
        tracts = repo.maximega_tcorc.census_tracts

        repo.dropCollection('income_with_tracts')
        repo.createCollection('income_with_tracts')

        # ----------------- Merge Census Tract info with AVG income per tract -----------------
        tract_with_income = {}
        for tract in tracts.find():
            for income in incomes.find():
                tract_num_income = income['tract']
                tract_num_income = tract_num_income[10:]
                # ----------------- Normalizing naming conventions for census tracts according to link below (bullets 4c & 4d in document) -----------------
                # ----------------- http://www.geo.hunter.cuny.edu/~amyjeu/gtech201/spring10/lab8_census.pdf -----------------
                if (tract_num_income[0:2] == '85'): tract_num_income = '5' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '81'): tract_num_income = '4' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '47'): tract_num_income = '3' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '05'): tract_num_income = '2' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '61'): tract_num_income = '1' + tract_num_income[2:]
                tract_num_tract = tract['BoroCT2010']
                # ----------------- If census tracts are equal, create new data object to load into DB -----------------
                if int(tract_num_income) == tract_num_tract:
                    # ----------------- Some tracts are missing income data so we ommit those from the merge -----------------
                    if income['income'] != None:
                        tract_with_income[tract_num_income] = {'income': income['income'], 'nta': tract['NTACode'], 'nta_name': tract['NTAName'], 'multi_polygon': tract['the_geom']}
        # ----------------- Reformat data for mongodb insertion -----------------
        insert_many_arr = []
        for key in tract_with_income.keys():
            insert_many_arr.append(tract_with_income[key])

        # ----------------- Data insertion into Mongodb ------------------
        repo.dropCollection('income_with_tracts')
        repo.createCollection('income_with_tracts')
        repo[repo_name].insert_many(insert_many_arr)
        repo[repo_name].metadata({'complete':True})
        print(repo[repo_name].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return None
