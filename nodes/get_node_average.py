import requests
import json
import time
import datetime
import operator
import statistics
import sys

# the versions we are intrested in
tracked_versions = ["1.4.0", "1.5.0", "1.5.1", "1.6.0"]

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
    resp = requests.get(url)
    if resp.status_code == 200:
        return json.loads(resp.text)
    else:
        raise Exception("unexpected response from charts.dcr.farm: HTTP status is " + str(resp.status_code))

def calc_node_version_stats(dcrfarm_data):

    useragent_means = []

    # convert data to structure like:
    # [["useragent1", "averagenodes1"], ["useragent2", "averagenodes2"], ...]
    for series in dcrfarm_data["results"][0]["series"]:
        ua = series["tags"]["useragent_tag"]
        mean = statistics.mean(map(operator.itemgetter(1), series["values"]))
        useragent_means.append([ua, mean])

    tracked_ua_means = []
    mean_sum = 0

    # filter out only useragents that contain strings from `tracked_versions`
    # also calculate the sum of all nodes into mean_sum
    for ua_mean in useragent_means:
        ua, mean = ua_mean
        for version in tracked_versions:
            if str(version) in str(ua):
                tracked_ua_means.append(ua_mean)
        mean_sum += mean

    # sort descending
    stats = sorted(tracked_ua_means, key=operator.itemgetter(1), reverse=True)

    # calculate percentages among total nodes and add them as a new column
    # [["useragent1", "averagenodes1", "average1%"], ...]
    for ua_mean in stats:
        ua, mean = ua_mean
        percentage = mean / (mean_sum / 100)
        ua_mean.append(percentage)

    return stats

def print_node_stats(stats, start_date):
    output = "Average version distribution for " + start_date.strftime("%B") + ": "
    dcrd_str = ""
    dcrwallet_str = ""
    tracked_percentage = 0

    # process and collect useragents strings
    for ua, avg, avgpc in stats:
        tracked_percentage += avgpc

        if "dcrd" in str(ua):
            ua_parts = ua.split("/")
            dcrd_str += str(round(avgpc, 2)) + "% " + ua_parts[2] + ", "

        if "dcrwallet" in str(ua):
            ua_parts = ua.split("/")
            dcrwallet_str += str(round(avgpc, 2)) + "% " + ua_parts[2] + ", "

    # build and print the final string
    output += dcrd_str + dcrwallet_str + str(round(100-tracked_percentage,2)) + "% Others."
    print(output)

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
