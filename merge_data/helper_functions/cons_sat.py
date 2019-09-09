import z3

def cons_sat(data_copy, k):
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
        S.add(val < .2, val > .01)

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
    S.add(parameters_z[-1] * 1.4 > parameters_z[0])

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
    