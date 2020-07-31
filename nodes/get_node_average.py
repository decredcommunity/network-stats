import requests
import json
import time
import datetime
import operator
import statistics
import sys
from collections import namedtuple

GroupStats = namedtuple("GroupStats", [
    "name",
    "avg_nodes",
    "avg_nodes_ratio",
    "stats"
])

Stats = namedtuple("Stats", [
    "group_stats",
    "untracked_ratio"
])

TRACKED_UA_GROUPS = {
    "dcrd v1.5.1":          ["/dcrwire:0.4.0/dcrd:1.5.1/"],
    "dcrd v1.5":            ["/dcrwire:0.4.0/dcrd:1.5.0/"],
    "dcrd v1.6 dev builds": ["/dcrwire:0.4.0/dcrd:1.6.0(pre)/"],
    "dcrd v1.5 dev and RC builds": [
        "/dcrwire:0.3.0/dcrd:1.5.0(pre)/",
        "/dcrwire:0.4.0/dcrd:1.5.0(pre)/",
        "/dcrwire:0.4.0/dcrd:1.5.0(rc1)/",
        "/dcrwire:0.4.0/dcrd:1.5.0(rc2)/",
    ],
    "dcrd v1.4":            ["/dcrwire:0.3.0/dcrd:1.4.0/"],
    "dcrwallet v1.5.1":     ["/dcrwire:0.4.0/dcrwallet:1.5.1+release/"],
    "dcrwallet v1.5":       ["/dcrwire:0.4.0/dcrwallet:1.5.0+release/"],
    "dcrwallet v1.4":       ["/dcrwire:0.3.0/dcrwallet:1.4.0+release/"],
}

def inverse_multidict(md):
    # compute an inverse multidict
    # multidict is a dict that maps keys to lists of elements
    # NOTE: there must be no duplicate elements across all lists!
    inverse = {}
    for k, v in md.items():
        for e in v:
            if e in inverse:
                raise Exception("duplicate elements in multidict values are not allowed")
            inverse[e] = k
    return inverse

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
    ua_stats = []
    mean_sum = 0
    get_count = operator.itemgetter(1)

    # convert data to structure like: [["useragent1", averagenodes1], ...]
    # also calculate the sum of average node counts
    for series in dcrfarm_data["results"][0]["series"]:
        ua = series["tags"]["useragent_tag"]
        mean = statistics.mean(map(get_count, series["values"]))
        ua_stats.append([ua, mean])
        mean_sum += mean

    # calculate ratios and add them as a new column
    # [["useragent1", averagenodes1, avgratio1], ...]
    for us in ua_stats:
        ua, mean = us
        ratio = mean / mean_sum
        us.append(ratio)

    # lookup dict to find a group name by user agent
    ua_to_group = inverse_multidict(TRACKED_UA_GROUPS)
    # temporary dict that maps group name to [["ua", averagenodes1, avgratio1], ...]
    grouped_stats = {}

    for uas in ua_stats:
        ua = uas[0]
        gname = ua_to_group.get(ua)
        if gname:
            # this ua belongs to a group we're interested in, so add it to the
            # list of stats for this group, creating new mapping if necessary
            gs = grouped_stats.setdefault(gname, [])
            gs.append(uas)

    group_stats = []
    tracked_ratio = 0

    for gname, gstats in grouped_stats.items():
        gavgnodes = sum(map(get_count, gstats))
        gavgratio = gavgnodes / mean_sum
        tracked_ratio += gavgratio
        group_stats.append(GroupStats(gname, gavgnodes, gavgratio, gstats))

    get_ratio = operator.itemgetter(2)
    group_stats_sorted = sorted(group_stats, key=get_ratio, reverse=True)

    stats = Stats(group_stats = group_stats_sorted,
                  untracked_ratio = (1 - tracked_ratio))
    return stats

def print_node_stats(stats, start_date):
    output = "Average version distribution for " + start_date.strftime("%B") + ": "
    dcrd_str = ""
    dcrwallet_str = ""

    # process and collect useragents strings
    for gs in stats.group_stats:
        gratio = gs.avg_nodes_ratio
        gname = gs.name
        if "dcrd" in gname:
            dcrd_str += str(round(gratio * 100, 2)) + "% " + gname + ", "

        if "dcrwallet" in gname:
            dcrwallet_str += str(round(gratio * 100, 2)) + "% " + gname + ", "

    # build and print the final string
    output += dcrd_str + dcrwallet_str + str(round((stats.untracked_ratio) * 100, 2)) + "% others."
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
