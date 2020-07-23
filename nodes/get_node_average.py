import requests
import json
import time
import datetime
import operator
import sys

#API endpoint
apipoint = "https://charts.dcr.farm/api/datasources/proxy/1/query?db=decred&"

#Change these dates for your range
start_date = datetime.datetime(2020,5,1,0,0,0,0)
end_date = datetime.datetime(2020,5,31,23,59,59,0)

# The versions we are intrested in
interested_versions_list = ["1.4.0","1.5.0","1.5.1","1.6.0"]

# Send request to API and return json data as json object
def get_data():

    #Convert datetime to unix time format as required by the api
    start_date_unix = str(start_date.replace(tzinfo=datetime.timezone.utc).timestamp())[:-2]+"000"
    end_date_unix = str(end_date.replace(tzinfo=datetime.timezone.utc).timestamp())[:-2]+"000"

    url = apipoint+'q=SELECT count(distinct("addr")) FROM "peers" WHERE time >= '+start_date_unix+ 'ms and time <= '+end_date_unix+'ms GROUP BY time(1d), "useragent_tag" fill(none)'
    print("===========================================================")
    print("Getting data from " + url)
    print("===========================================================")
    response = requests.get(url)
    if response.status_code == 200:
        response_json = json.loads(response.text)
        return response_json
    else:
        print("Check Connection")
        exit

def print_node_data(interested_useragents_percentage):
    print("===========================================================")

    print_list = "Average version distribution for " + start_date.strftime('%B') + ": "

    intrested_percentage_count = 0

    # Process and print dcrd useragents.
    for useragent in interested_useragents_percentage:
        intrested_percentage_count = useragent[0][2] + intrested_percentage_count
        if "dcrd" in str(useragent[0][0]):
            templist = useragent[0][0].split("/")
            print_list= print_list + str(round(useragent[0][2],2)) + "%  " + templist[2] + ", "

    # Process and print dcrwallet useragents. 
    for useragent in interested_useragents_percentage:
        if "dcrwallet" in str(useragent[0][0]):
            templist = useragent[0][0].split("/")
            print_list= print_list +  str(round(useragent[0][2],2)) + "%  " + templist[2] + ", "

    # Print Others
    print_list = print_list + str(round(100-intrested_percentage_count,2)) + "%  " + "Others."

    
    print(print_list)
    print("===========================================================")

filename = sys.argv[1] if len(sys.argv) > 1 else None
if filename:
    print("reading from file")
    with open(filename) as f:
        get_data_json = json.load(f)
else:
    #Get the data as JSON from the API endpoint
    get_data_json = get_data()

#Uncomment to get raw json
#print(json.dumps(get_data_json))

useragent_avg_list = []

#Process json into list of format [['useragent','averagenodes'],['useragent2','averagenodes2']...]
for series in get_data_json['results'][0]['series']:
    total = 0
    count = 0
    data_useragent = series['tags']['useragent_tag']
    for data_point in series['values']:
        total = total + data_point[1]
        count = count+1
    average=total/count
    useragent_avg_list.append([data_useragent,average])


interested_useragents = []
totalcount = 0

# Filter out only useragents that contain strings form `interested_versions_list` also calculate the sum of all interested nodes into totalcount.
for useragent in useragent_avg_list:
    for version in interested_versions_list:
        if str(version) in str(useragent[0]):
            interested_useragents.append(useragent)
    totalcount = totalcount + useragent[1]


#Sort decending
interested_useragent_ordered = sorted(interested_useragents, key=operator.itemgetter(1), reverse=True)


interested_useragents_percentage = []

#Calculate and add another column into list. [['useragent','averagenodes','average%'],['useragent2','averagenodes2',average2%]...]
for intrest in interested_useragent_ordered:
    percentage = intrest[1]/(totalcount/100)
    intrest.append(percentage)
    interested_useragents_percentage.append([intrest])

#Send data into print_node_data function to output in desired format.
print_node_data(interested_useragents_percentage)
