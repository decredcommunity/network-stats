import requests
import json
import time
import operator
import statistics
import sys
from collections import namedtuple
from datetime import datetime, timezone

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
    "dcrd v1.5.2":          ["/dcrwire:0.4.0/dcrd:1.5.2/"],
    "dcrd v1.5.1":          ["/dcrwire:0.4.0/dcrd:1.5.1/"],
    "dcrd v1.5.0":          ["/dcrwire:0.4.0/dcrd:1.5.0/"],
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

KNOWN_UAS_FILE = "user-agents.list"

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
    # datetime.timestamp(), which might return an incorrect Unix
    # timestamp if the system timezone is not UTC
    assert dt.tzinfo == timezone.utc

    return int(dt.timestamp()) * 1000

# send request to dcr.farm API and return JSON data as a Python object
def get_dcrfarm_data(start_date, end_date):

    # convert datetime to Unix milliseconds as required by the API
    start_unix_ms = datetime_to_unix_millis(start_date)
    end_unix_ms = datetime_to_unix_millis(end_date)

    url = ('https://charts.dcr.farm/api/datasources/proxy/1/query?db=decred&q='
           'SELECT count(distinct("addr")) FROM "peers"'
           ' WHERE time >= {start_ms}ms and time < {end_ms}ms'
           ' GROUP BY time(1d), "useragent_tag" fill(none)'
          ).format(start_ms = start_unix_ms, end_ms = end_unix_ms)

    print("fetching " + url)
    resp = requests.get(url)
    if resp.status_code == 200:
        return json.loads(resp.text)
    else:
        raise Exception("unexpected response from charts.dcr.farm: "
                        "HTTP status is " + str(resp.status_code))

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
    group_stats_sorted = sorted(group_stats, key = get_ratio, reverse = True)

    stats = Stats(group_stats = group_stats_sorted,
                  untracked_ratio = (1 - tracked_ratio))
    return stats

def concise_round(f):
    """Round a floating point number for a concise presentation.

    The rounding removes decimal places one by one while the result is less
    than 10% away from the original.

    The intent is to reduce the visual noise of too many decimal places, but
    keep enough while they make significance.

    NOTE: This is using Python's standard `round` function, which uses "round
    half to even", e.g. 10.5 becomes 10 and not 11.
    """
    sensitivity = 0.1 # 10%
    maxdigits = 2 # start with rounding to 2 decimal digits

    res = f
    digits = maxdigits

    while digits >= 0:
        cand = round(f, digits)
        # rounded number can be lower or greater than original
        change = abs(1 - cand / f)
        if change > sensitivity:
            return res
        res = cand
        digits -= 1

    # cannot round further after all decimal digits are removed

    intres = int(res)
    if intres == res:
        # int is convenient as its str() is more concise ("10" vs "10.0")
        return intres
    else:
        raise Exception("fully rounded float not equal to int, too bad!")

def fmt_percent(f):
    return str(concise_round(f * 100)) + "%"

def print_node_stats(stats, start_date):
    output = "Average version distribution for " + start_date.strftime("%B") + ": "
    dcrd_str = ""
    dcrwallet_str = ""

    # process and collect useragents strings
    for gs in stats.group_stats:
        gratio = gs.avg_nodes_ratio
        gname = gs.name
        if "dcrd" in gname:
            dcrd_str += fmt_percent(gratio) + " " + gname + ", "

        if "dcrwallet" in gname:
            dcrwallet_str += fmt_percent(gratio) + " " + gname + ", "

    # build and print the final string
    output += dcrd_str + dcrwallet_str + fmt_percent(stats.untracked_ratio) + " others."
    print(output)

def load_list(path):
    with open(path) as f:
        return [line.rstrip("\n") for line in f]

def save_list(iterable, path):
    with open(path, "w", newline = "\n") as f:
        for item in iterable:
            f.write(str(item))
            f.write("\n")
    print("saved: " + path)

def update_user_agents(dcrfarm_data):
    """Add any new user agents to the known list."""
    import os

    known_uas = set()
    if os.path.isfile(KNOWN_UAS_FILE):
        known_uas = set(load_list(KNOWN_UAS_FILE))
        print("loaded {} user agents from {}".format(len(known_uas), KNOWN_UAS_FILE))

    uas = set()
    for series in dcrfarm_data["results"][0]["series"]:
        ua = series["tags"]["useragent_tag"]
        uas.add(ua)

    new_uas = uas.difference(known_uas)
    if new_uas:
        print("found {} new user agents not seen before:".format(len(new_uas)))
        for ua in sorted(new_uas):
            print(ua)
        new_known_uas = known_uas.union(new_uas)
        save_list(sorted(new_known_uas), KNOWN_UAS_FILE)
    else:
        print("no new user agents found")

def inc_month(dt):
    if dt.month < 12:
        return dt.replace(month = dt.month + 1)
    else:
        return dt.replace(year = dt.year + 1, month = 1)

def dec_month(dt):
    if dt.month > 1:
        return dt.replace(month = dt.month - 1)
    else:
        return dt.replace(year = dt.year - 1, month = 12)

def month_range(dt):
    # make two datetimes that are start and end of dt's month
    # the end datetime is start of the next month
    # produce aware UTC datetimes as required by datetime_to_unix_millis
    start = datetime(dt.year, dt.month, 1, tzinfo = timezone.utc)
    end = inc_month(start)
    return (start, end)

def load_json(filename):
    with open(filename) as f:
        return json.load(f)

def save_json(obj, filename):
    try:
        with open(filename, "x") as f:
            json.dump(obj, f, sort_keys = True, indent = 2)
            f.write("\n")
        print("saved: " + filename)
    except FileExistsError as e:
        print("failed to save: file exists: " + filename)

def make_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(description = "Decred node stats tool")

    parser.add_argument("-m", "--month",
                        help = "month to report (previous month by default), "
                               "formatted as YYYYMM, e.g. 202008")
    parser.add_argument("-i", "--in-file",
                        help = "do not make request, read input from file")
    parser.add_argument("-s", "--save-response",
                        dest = "resp_file",
                        help = "save response JSON to file")
    parser.add_argument("-u", "--update-uas",
                        action = "store_true",
                        help = "update {} with new user agents instead of "
                               "printing main stats".format(KNOWN_UAS_FILE))

    return parser

def main():
    parser = make_arg_parser()
    args = parser.parse_args()

    if args.month:
        mdate = datetime.strptime(args.month, "%Y%m").replace(tzinfo = timezone.utc)
    else:
        now = datetime.now(timezone.utc)
        mdate = dec_month(now)
        print("assuming month: " + mdate.strftime("%B %Y") + " (use -m to change)")

    start_date, end_date = month_range(mdate)

    if args.in_file:
        dcrfarm_data = load_json(args.in_file)
    else:
        # get the data as from the API endpoint
        dcrfarm_data = get_dcrfarm_data(start_date, end_date)

    if args.resp_file:
        save_json(dcrfarm_data, args.resp_file)

    if args.update_uas:
        update_user_agents(dcrfarm_data)
    else:
        stats = calc_node_version_stats(dcrfarm_data)
        # print the stats in desired format.
        print_node_stats(stats, start_date)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error: " + str(e))
        sys.exit(1)
