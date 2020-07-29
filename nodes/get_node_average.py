import requests
import json
import time
import datetime
import operator
import statistics
import sys

# The versions we are intrested in
interested_versions_list = ["1.4.0", "1.5.0", "1.5.1", "1.6.0"]

def datetime_to_unix_millis(dt):
    # require an aware UTC date so that there is no room for error when calling
    # datetime.datetime.timestamp(), which might return an incorrect Unix
    # timestamp if the system timezone is not UTC
    assert dt.tzinfo == datetime.timezone.utc

    return int(dt.timestamp()) * 1000

# Send request to API and return json data as json object
def get_dcrfarm_data(start_date, end_date):

    #Convert datetime to unix time format as required by the api
    start_unix_ms = datetime_to_unix_millis(start_date)
    end_unix_ms = datetime_to_unix_millis(end_date)

    url = ('https://charts.dcr.farm/api/datasources/proxy/1/query?db=decred&q='
           'SELECT count(distinct("addr")) FROM "peers"'
           ' WHERE time >= {start_ms}ms and time <= {end_ms}ms'
           ' GROUP BY time(1d), "useragent_tag" fill(none)'
          ).format(start_ms=start_unix_ms, end_ms=end_unix_ms)

    print("===========================================================")
    print("Getting data from " + url)
    print("===========================================================")
    response = requests.get(url)
    if response.status_code == 200:
        response_json = json.loads(response.text)
        return response_json
    else:
        raise Exception("unexpected response from charts.dcr.farm: HTTP status is " + str(response.status_code))

def calc_node_version_stats(dcrfarm_data):

    useragent_avg_list = []

    #Process json into list of format [["useragent","averagenodes"],["useragent2","averagenodes2"]...]
    for series in dcrfarm_data["results"][0]["series"]:
        data_useragent = series["tags"]["useragent_tag"]
        mean = statistics.mean(map(operator.itemgetter(1), series["values"]))
        useragent_avg_list.append([data_useragent, mean])

    interested_useragents = []
    totalcount = 0

    # Filter out only useragents that contain strings from `interested_versions_list` also calculate the sum of all nodes into totalcount.
    for ua_mean in useragent_avg_list:
        ua, mean = ua_mean
        for version in interested_versions_list:
            if str(version) in str(ua):
                interested_useragents.append(ua_mean)
        totalcount += mean

    #Sort decending
    interested_useragent_ordered = sorted(interested_useragents, key=operator.itemgetter(1), reverse=True)

    #Calculate and add another column into list. [["useragent","averagenodes","average%"],["useragent2","averagenodes2",average2%]...]
    for intrest in interested_useragent_ordered:
        percentage = intrest[1] / (totalcount / 100)
        intrest.append(percentage)

    return interested_useragent_ordered

def print_node_stats(interested_useragents_percentage, start_date):
    print("===========================================================")

    print_list = "Average version distribution for " + start_date.strftime("%B") + ": "

    intrested_percentage_count = 0

    # Process and print dcrd useragents.
    for useragent in interested_useragents_percentage:
        intrested_percentage_count += useragent[2]
        if "dcrd" in str(useragent[0]):
            templist = useragent[0].split("/")
            print_list += str(round(useragent[2], 2)) + "%  " + templist[2] + ", "

    # Process and print dcrwallet useragents. 
    for useragent in interested_useragents_percentage:
        if "dcrwallet" in str(useragent[0]):
            templist = useragent[0].split("/")
            print_list +=  str(round(useragent[2], 2)) + "%  " + templist[2] + ", "

    # Print Others
    print_list += str(round(100-intrested_percentage_count,2)) + "%  " + "Others."

    
    print(print_list)
    print("===========================================================")

def main():
    #Change these dates for your range
    start_date = datetime.datetime(2020, 5,  1,  0,  0,  0,  0, tzinfo=datetime.timezone.utc)
    end_date   = datetime.datetime(2020, 5, 31, 23, 59, 59,  0, tzinfo=datetime.timezone.utc)

    filename = sys.argv[1] if len(sys.argv) > 1 else None
    if filename:
        print("reading from file")
        with open(filename) as f:
            dcrfarm_data = json.load(f)
    else:
        #Get the data as JSON from the API endpoint
        dcrfarm_data = get_dcrfarm_data(start_date, end_date)

    #Uncomment to get raw json
    #print(json.dumps(dcrfarm_data))

    stats = calc_node_version_stats(dcrfarm_data)
    #Send data into print_node_stats function to output in desired format.
    print_node_stats(stats, start_date)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error: " + str(e))
        sys.exit(1)
