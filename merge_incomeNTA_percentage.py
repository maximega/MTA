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
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        #agent
        this_script = doc.agent('alg:maximega_tcorc#merge_incomeNTA_percentage', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        #resource
        trans_percent = doc.entity('dat:maximega_tcorc#transportation_percentages', {prov.model.PROV_LABEL:'Percentage of Neighborhood commuters who take public transportation', prov.model.PROV_TYPE:'ont:DataSet'})
        income_with_neighborhoods = doc.entity('dat:maximega_tcorc#income_with_neighborhoods', {prov.model.PROV_LABEL:'NYC Neighborhoods + Avg census income', prov.model.PROV_TYPE:'ont:DataSet'})
        #agent
        merging_percent_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(merging_percent_income, this_script)

        doc.usage(merging_percent_income, trans_percent, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Computation'
                    }
                    )
        doc.usage(merging_percent_income, income_with_neighborhoods, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Computation'
                    }
                    )
        #reasource
        percent_with_income = doc.entity('dat:maximega_tcorc#income_with_NTA_with_percentages', {prov.model.PROV_LABEL:'NYC NTA Info + Public Transportation Percentage', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(percent_with_income, this_script)
        doc.wasGeneratedBy(percent_with_income, merging_percent_income, endTime)
        doc.wasDerivedFrom(percent_with_income, trans_percent, merging_percent_income, merging_percent_income, merging_percent_income)
        doc.wasDerivedFrom(percent_with_income, income_with_neighborhoods, merging_percent_income, merging_percent_income, merging_percent_income)

        repo.logout()
                
        return doc