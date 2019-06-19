import dml
import prov.model
import z3

from helper_functions.lat_long_kmeans import run_lat_long_kmeans

class get_app_data(dml.Algorithm):

    contributor = 'maximega_tcorc'
    reads = ['maximega_tcorc.income_with_NTA_with_percentages']
    writes = []

    @staticmethod
    def execute(trial = False):
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # -----------------Retrieve neighborhood data from Mongodb -----------------
        nta_objects = repo.maximega_tcorc.income_with_NTA_with_percentages.find()

        return nta_objects

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return None

def run(zones, maxp, minp, f):
    
    nta_objects = get_app_data.execute()
    incomes = []
    pops = []
    X = []
    data_copy = []
    # ----------------- Create a copy of the data, only including neihgborhoods with a subway station -----------------
    for nta in nta_objects:
        if(len(nta['stations'])!= 0):
            income = nta['income']
            X.append([nta['ntaname'], nta['position'][0], nta['position'][1], income])
            data_copy.append(nta)
            incomes.append(income)
            pops.append(nta['trans_percent'])

    # ----------------- Values added dynamically from app -----------------

    k = zones
    percent_max = maxp/100
    percent_min = minp/100
    factor = f
    #------------------ Run k Means on ([lat, long], avg_income) -----------------
    kmeans = run_lat_long_kmeans(X, k)

    k_groupings = kmeans.labels_
    for i in range(len(data_copy)):
        data_copy[i]['zone'] = k_groupings[i]
    avg_inc = [0] * k
    count_inc = [0] * k

    # ----------------- Find and insert average income for each zone -----------------
    for item in data_copy:
        avg_inc[item['zone']] += item['income']
        count_inc[item['zone']] += 1
    for i in range(len(avg_inc)):
        avg_inc[i] /= count_inc[i]
    for i in range(len(data_copy)):
        data_copy[i]['avg_inc'] = avg_inc[data_copy[i]['zone']]
    # ----------------- Reorder the zones based on avg_zone_income (income zone1 > income zone 2 ...) -----------------
    for i in range(1, k +1):
        max_avg = max(avg_inc)
        for item in data_copy:
            if (item['avg_inc'] == max_avg):
                item['zone'] = i
        avg_inc.remove(max_avg)
    # ----------------- Use z3 to find new zone fares that satisfy constraint set -----------------	
    new_fares = cons_sat(data_copy, k, percent_max, percent_min, factor)

    # ----------------- If constraint set was not satisfied, do not insert into mongodb -----------------
    if (new_fares != 'unsat'):
        # ----------------- Insert new zone fares into Mongodb -----------------
        for item in data_copy:
            item['routes'] = []
            for i in range(len(new_fares)):
                key = list(new_fares[i].keys())[0]
                dest_1 = int(key[1])
                dest_2 = int(key[-1])
                if item['zone'] == dest_1 or item['zone'] == dest_2:
                    x = new_fares[i][key]
                    obj = {}
                    obj[key] = x
                    item['routes'].append(obj)
        return data_copy

    if (new_fares == 'unsat'):
        return []

def cons_sat(data_copy, k, percent_max, percent_min, factor):

    # ----------------- Each parameter will look like this (r1-1, r1-2, ..., rk-k) -----------------
    # ----------------- For example, r2-4 represents a route where the rider mey exit/enter in zone 2 and exit/enter in zone 4 -----------------
    parameter_names_z = []
    for i in range(1, k + 1):
        for j in range(i, k + 1):
            val = "r{0:d}".format(i)
            val += "-{0:d}".format(j)
            parameter_names_z.append(val)
    parameters_z = [z3.Real(n) for n in parameter_names_z]

    S = z3.Solver()

    projected_route_total = [0] * len(parameter_names_z)
    real_route_total = 0

    for item in data_copy:
        for i in range(len(parameter_names_z)):
            dest_1 = int(parameter_names_z[i][1])
            dest_2 = int(parameter_names_z[i][-1])
            # ----------------- Find daily revenue based on data -----------------
            # ----------------- This number will be the sum of the revenue from all route combinations -----------------
            if item['zone'] == dest_1 or item['zone'] == dest_2:
                projected_route_total[i] += (float(item['population']) * (item['trans_percent'] / 100))
                real_route_total += float(item['population']) * (item['trans_percent'] / 100) * 2.75
    

    real_route_total = int(real_route_total)

    val = 0
    for i in range(len(parameters_z)):
        val += (parameters_z[i] * projected_route_total[i])
    # ----------------- Ensure that new fares for each route still == overall real revenue  -----------------
    S.add(val == real_route_total)

    for i in range(len(parameters_z)):
        val = (parameters_z[i] * projected_route_total[i])/real_route_total
        # ----------------- Enusre new fares dont make one route responsible for more than 20% of revenue or less than 1% revenue -----------------
        S.add(val < percent_max, val > percent_min)

    # ----------------- Ensure that routes between two zones with lower avg incomes pay less than routes between two zones with higher average income -----------------
    for i in range(len(parameters_z)):
        outer_1 = int(parameter_names_z[i][1])
        outer_2 = int(parameter_names_z[i][-1])
        for j in range(i, len(parameters_z)):
            inner_1 = int(parameter_names_z[j][1])
            inner_2 = int(parameter_names_z[j][-1])
            # ----------------- E.g: r1-1 (1+1) < r3-4 (3+4); therefore r1-1 will have a higher fare than r3-4 -----------------
            if (outer_1 + outer_2 < inner_1 + inner_2):
                S.add(parameters_z[i] > parameters_z[j])
            # ----------------- Opposite of above -----------------
            elif (outer_1 + outer_2 > inner_1 + inner_2):
                S.add(parameters_z[i] < parameters_z[j])

    # ----------------- Ensure that the most expesnive route (r1-1) is at most 1.4 times the cheapest route (r5-5) -----------------
    S.add(parameters_z[-1] * factor > parameters_z[0])

    if str(S.check()) == 'unsat':
        return 'unsat'
    else:
        sat = S.model()
        new_fares = [] 
        for i in range(len(sat)):
            route = sat[i].name()
            price = sat[sat[i]].as_decimal(2)
            if price[-1] == '?':
                price = price[:-1]
            
            temp_dict = {}
            temp_dict[route] = price
            new_fares.append(temp_dict)
        return new_fares
