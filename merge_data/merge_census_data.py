import datetime
import json
import pymongo
import urllib.request
import uuid

class merge_census_data():
    reads = ['mta.census_tracts', 'mta.census_income']
    writes = ['mta.income_with_tracts']


    @staticmethod
    def execute():
        startTime = datetime.datetime.now()

        repo_name = merge_census_data.writes[0]
        # ----------------- Set up the database connection -----------------
        client = pymongo.MongoClient()
        repo = client.repo
        # ----------------- Retrieve data from Mongodb -----------------
        incomes = repo.mta.census_income
        tracts = repo.mta.census_tracts

        repo.drop_collection('income_with_tracts')
        repo.create_collection('income_with_tracts')
        # ----------------- Merge Census Tract info with AVG income per tract -----------------
        tract_with_income = {}
        i = 0 
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
        repo.drop_collection(repo_name)
        repo.create_collection(repo_name)
        repo[repo_name].insert_many(insert_many_arr)
        
        repo.logout()

        endTime = datetime.datetime.now()

        print(repo_name, "completed")
