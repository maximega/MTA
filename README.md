How to Run Web App:
  1.) Run Flask app (python app.py) make sure to run on 8080
  2.) Open a chrome browser with security disabled (CORS issue):

  For PC: chrome.exe --user-data-dir="C://Chrome dev session" --disable-web-security
  For Mac: open -n -a /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --args --user-data-dir="/tmp/chrome_dev_test" --disable-web-security

  3.) Open index.html inside that chrome browser (whole AngularJS front end is in this file; all js libraries are accessed with CDN's)  
  4.) If that doesn't work, either try quitting chrome, or using Firefox with a CORS plugin

Data Sets Used:

  https://api.datausa.io/api/?sort=desc&show=geo&required=income&sumlevel=tract&year=all&where=geo%3A16000US3651000
  http://datamechanics.io/data/maximega_tcorc/NYC_census_tracts.csv
  http://datamechanics.io/data/maximega_tcorc/NYC_subway_exit_entrance.csv
  https://data.cityofnewyork.us/resource/swpk-hqdp.json     
  https://data.cityofnewyork.us/resource/q2z5-ai38.json
  
Data Portals Used:

   https://data.cityofnewyork.us/       
   https://datamechanics.io/       
   https://datausa.io       
   
Non-trivial Data Transformations:

  1: Merging economic with geographical census information, creating a new data set that has a census tract as a key and (average income per tract, neighborhood that the tract belongs to, and multipolygon coordinates) as a value.
  
  2: Merging subway station information with neighborhood (NTA) information, creating a new data set that has an NTA code as a key and (multipolygon coordinates, and (station name, subway line, and coordinates (lat, long) of the station) as a value.
  
  3: Merging neighborhood population information with (2), creating a new data set that has an NTA code as a key, and (name, multipolygon coordinates, station information, and population) as a value.
 
  4: Merging (1) with (3), creating a new data set that has an NTA code as a key, and a (name, station information, and population, and census information) as a value.
  
Problem to Solve:
  
  The subway is an essential mode of transportation in New York City. Life in NYC has gotten far more expensive, and the subway is no exception; a ride fare has increased 37.5% (from $2.00 to $2.75) in the last 16 years. To help make life more affordable, we thought it could be worthwhile to rethink subway fares. Taking the London Underground zoning system as an example, we thought that maybe NYC subways could benifit from fare zones. In London the cost of a subway ride depends on the distance of your travel (how many zones you cross). We thought that we could rezone the subway system, but instead of creating zones based on distance, we would create fare zones based on socio-economic data from each NYC neighborhood (NTA). In the future, we may also like to look deeper into the volume and type of travel on (busses, taxi, ubers, etc...) vs subways, and the most frequent routes taken by subway riders. The goal would be for people from less afluent neighborhoods pay less for a subway swipe than those from more affluent neighborhoods, all while ensuring that the MTA would not lose money in this venture. 


K-Means:


  We are using k-means on latitude, longitude, and income of each neighborhood to determine the zones for the fare changes. To be able to run k-means with these variables, we first had to scale them all into numbers in the range between 0-1 using the MinMaxScaler. After running k-means on this scaled data, we plotted the error as a function of k to see which value of k minimized the error. Then, we plotted the neighborhoods by their original un-scaled latitude and longitude points, color-coded with their respective zones. Using latitude and longitude in addition to income for k-means gives a better grouping for the zones instead of basing it solely off of income. 


Constraint Satisfaction:


  We then created a set of constraints to find the new fares for each of the zones. We utilized a z3-solver to help find a solution to our constraint satisfaction problem. The first constraint we set was that the sum of each of the k fares when multiplied by the number of riders per each zone must equal the real overall revenue for the normal $2.75 fare rides. We then made sure that there was a balance in revenue between the zones by creating a constraint in which the new fares can not create a zone that accounts for more than 30% of daily MTA revenue. Similarly we put the opposite constraint to ensure that no zone accounts for less than 5% of daily revenue. Another constraint we set was that no fare can be greater than 1.25 times any other fare. We then ensured that zone 1 was the zone with the lowest average income, …, and zone k would be the zone with the highest average income. The last constraint we made was that (fare zone 1 < fare zone 2, …, fare zone k-1 < fare zone k). For further information on the constraint satisfaction please see the commented code. 

Statistical Analysis:


  For statistical analysis, we computed the correlation coefficient and p-value between the average income and the corresponding percent of the population of that neighborhood using the subway. We found that they were negatively correlated, seeing as our p value was greater than the coefficient. Therefore, as the average income increases, the percent of people using subway decreases. To find out whether or not this was significant, we computed the absolute value of the coefficient and compared it to our p-value. Seeing as our p-value was lower than that absolute value, our coefficient was significant. 
  

Trial Mode for Project #2:
  
  In kmeans_opt.py
