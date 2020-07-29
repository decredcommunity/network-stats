import requests
import json
import time
import datetime
import operator
import statistics
import sys

# the versions we are intrested in
interested_versions_list = ["1.4.0", "1.5.0", "1.5.1", "1.6.0"]

def datetime_to_unix_millis(dt):
    # require an aware UTC date so that there is no room for error when calling
    # datetime.datetime.timestamp(), which might return an incorrect Unix
    # timestamp if the system timezone is not UTC
    assert dt.tzinfo == datetime.timezone.utc

    return int(dt.timestamp()) * 1000

# send request to dcr.farm API and return JSON data as a Python object
def get_dcrfarm_data(start_date, end_date):

    # convert datetime to Unix milliseconds as required by the API
    start_unix_ms = datetime_to_unix_millis(start_date)
    end_unix_ms = datetime_to_unix_millis(end_date)

    url = ('https://charts.dcr.farm/api/datasources/proxy/1/query?db=decred&q='
           'SELECT count(distinct("addr")) FROM "peers"'
           ' WHERE time >= {start_ms}ms and time <= {end_ms}ms'
           ' GROUP BY time(1d), "useragent_tag" fill(none)'
          ).format(start_ms=start_unix_ms, end_ms=end_unix_ms)

    print("fetching " + url)
    response = requests.get(url)
    if response.status_code == 200:
        response_json = json.loads(response.text)
        return response_json
    else:
        raise Exception("unexpected response from charts.dcr.farm: HTTP status is " + str(response.status_code))

def calc_node_version_stats(dcrfarm_data):

    useragent_avg_list = []

    # convert data to structure like:
    # [["useragent1", "averagenodes1"], ["useragent2", "averagenodes2"], ...]
    for series in dcrfarm_data["results"][0]["series"]:
        data_useragent = series["tags"]["useragent_tag"]
        mean = statistics.mean(map(operator.itemgetter(1), series["values"]))
        useragent_avg_list.append([data_useragent, mean])

    interested_useragents = []
    totalcount = 0

    # filter out only useragents that contain strings from `interested_versions_list`
    # also calculate the sum of all nodes into totalcount
    for ua_mean in useragent_avg_list:
        ua, mean = ua_mean
        for version in interested_versions_list:
            if str(version) in str(ua):
                interested_useragents.append(ua_mean)
        totalcount += mean

    # sort descending
    interested_useragent_ordered = sorted(interested_useragents, key=operator.itemgetter(1), reverse=True)

    # calculate percentages among total nodes and add them as a new column
    # [["useragent1", "averagenodes1", "average1%"], ...]
    for intrest in interested_useragent_ordered:
        percentage = intrest[1] / (totalcount / 100)
        intrest.append(percentage)

    return interested_useragent_ordered

def print_node_stats(interested_useragents_percentage, start_date):
    print_list = "Average version distribution for " + start_date.strftime("%B") + ": "
    dcrd_str = ""
    dcrwallet_str = ""
    intrested_percentage_count = 0

    # process and collect useragents strings
    for ua, avg, avgpc in interested_useragents_percentage:
        intrested_percentage_count += avgpc

        if "dcrd" in str(ua):
            templist = ua.split("/")
            dcrd_str += str(round(avgpc, 2)) + "% " + templist[2] + ", "

        if "dcrwallet" in str(ua):
            templist = ua.split("/")
            dcrwallet_str += str(round(avgpc, 2)) + "% " + templist[2] + ", "

    # build and print the final string
    print_list += dcrd_str + dcrwallet_str + str(round(100-intrested_percentage_count,2)) + "% Others."
    print(print_list)

def main():
    # change these dates for your time period
    start_date = datetime.datetime(2020, 5,  1,  0,  0,  0,  0, tzinfo=datetime.timezone.utc)
    end_date   = datetime.datetime(2020, 5, 31, 23, 59, 59,  0, tzinfo=datetime.timezone.utc)

    filename = sys.argv[1] if len(sys.argv) > 1 else None
    if filename:
        print("reading from file")
        with open(filename) as f:
            dcrfarm_data = json.load(f)
    else:
        # get the data as from the API endpoint
        dcrfarm_data = get_dcrfarm_data(start_date, end_date)

    # uncomment to print raw JSON
    #print(json.dumps(dcrfarm_data))

    stats = calc_node_version_stats(dcrfarm_data)
    # print the stats in desired format.
    print_node_stats(stats, start_date)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error: " + str(e))
        sys.exit(1)
