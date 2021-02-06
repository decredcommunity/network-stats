import requests
import json
import time
import operator
import os
import statistics
import sys
from collections import namedtuple
from datetime import date, datetime, timezone

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

DCRFARM_START_DATE = date(2017, 12, 1)
TRACKED_UAS_FILE = "user-agents-tracked.json"
KNOWN_UAS_FILE = "user-agents.list"

def inverse_multidict(md):
    # compute an inverse multidict
    # multidict is a dict that maps keys to lists of elements
    # NOTE: there must be no duplicate elements across all lists!
    inverse = {}
    for k, v in md.items():
        for e in v:
            if e in inverse:
                raise ValueError("duplicate multidict values are not allowed: "
                                 + str(e))
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

    print("getting data from {} till {}".format(start_date, end_date))
    print("fetching " + url)
    resp = requests.get(url)
    if resp.status_code == 200:
        return json.loads(resp.text)
    else:
        raise Exception("unexpected response from charts.dcr.farm: "
                        "HTTP status is " + str(resp.status_code))

def calc_node_stats(dcrfarm_data):
    ua_stats = []
    daily_mean_sum = 0
    get_count = operator.itemgetter(1)

    # convert data to structure like: [["useragent1", dailymean1], ...]
    # also calculate the sum of average node counts
    for series in dcrfarm_data["results"][0]["series"]:
        ua = series["tags"]["useragent_tag"]
        # get daily counts of distinct addrs for each UA
        daily_counts = list(map(get_count, series["values"]))
        daily_mean = statistics.mean(daily_counts)
        daily_mean_sum += daily_mean
        ua_stats.append([ua, daily_mean])

    # calculate ratios and add them as a new column
    # [["useragent1", dailymean1, dailymeanratio1], ...]
    for us in ua_stats:
        ua, daily_mean = us
        daily_ratio = daily_mean / daily_mean_sum # the ratio is unused yet
        us.append(daily_ratio)

    return ua_stats, daily_mean_sum

def calc_node_group_stats(ua_stats, daily_mean_sum):
    if os.path.isfile(TRACKED_UAS_FILE):
        tracked_ua_groups = load_json(TRACKED_UAS_FILE)
        print("loaded {} UA groups from {}".format(
            len(tracked_ua_groups), TRACKED_UAS_FILE))
    else:
        raise Exception("file not found: " + TRACKED_UAS_FILE)
    # lookup dict to find a group name by user agent
    ua_to_group = inverse_multidict(tracked_ua_groups)

    # temporary dict that maps group name to [["ua", dailymean1, dailymeanratio1], ...]
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

    get_mean = operator.itemgetter(1)

    for gname, gstats in grouped_stats.items():
        # get daily means of the group
        gdaily_means = list(map(get_mean, gstats))
        gdaily_mean_sum = sum(gdaily_means)
        gdaily_ratio = gdaily_mean_sum / daily_mean_sum
        tracked_ratio += gdaily_ratio
        group_stats.append(GroupStats(gname, gdaily_mean_sum, gdaily_ratio, gstats))

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

def print_node_counts(ua_stats, daily_mean_sum):
    print("{:44}|{:10}|{:11}".format("user agent", "daily mean", "daily ratio"))
    print("{:-<44}|{:-<10}|{:-<11}".format("", "", ""))
    for ua, daily_mean, daily_ratio in sorted(ua_stats, key=operator.itemgetter(1)):
        print("{:43} |{:>9.1f} |{:>10.2%} ".format(ua, daily_mean, daily_ratio))
    print("sum of daily means: {:.1f}".format(daily_mean_sum))

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

def month_interval(dt):
    # make two datetimes that are start and end of dt's month
    # the end datetime is start of the next month
    # produce aware UTC datetimes as required by datetime_to_unix_millis
    start = datetime(dt.year, dt.month, 1, tzinfo = timezone.utc)
    end = inc_month(start)
    return (start, end)

def month_range(start, end):
    """Generate a sequence of months. Args must be datetime.date."""
    month = date(start.year, start.month, 1)
    while month < end:
        yield month
        month = inc_month(month)

def load_json(filename):
    try:
        with open(filename) as f:
            return json.load(f)
    except Exception as e:
        msg = e.args[0] if len(e.args) > 0 else ""
        raise Exception(filename + ": " + msg) from e

def save_json(obj, filename):
    try:
        with open(filename, "x") as f:
            json.dump(obj, f, sort_keys = True, indent = 2)
            f.write("\n")
        print("saved: " + filename)
    except FileExistsError as e:
        print("failed to save: file exists: " + filename)

def find_free_filename(name, ext):
    filename = name + ext
    n = 1
    while os.path.exists(filename):
        filename = name + "." + str(n) + ext
        n += 1
    return filename

def save_range(start, end):
    for month in month_range(start, end):
        mstart, mend = month_interval(month)
        # get the data as from the API endpoint
        dcrfarm_data = get_dcrfarm_data(mstart, mend)
        monthstr = month.strftime("%Y%m")
        filename = find_free_filename(monthstr, ".json")
        save_json(dcrfarm_data, filename)
        time.sleep(2)

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
    parser.add_argument("-p", "--print-counts",
                        action = "store_true",
                        help = "print mean daily count for each UA")
    parser.add_argument("-u", "--update-uas",
                        action = "store_true",
                        help = "update {} with new user agents instead of "
                               "printing main stats".format(KNOWN_UAS_FILE))
    parser.add_argument("-a", "--save-all",
                        action = "store_true",
                        help = "save JSON file for each month between {} and today"
                            .format(DCRFARM_START_DATE))

    return parser

def main():
    parser = make_arg_parser()
    args = parser.parse_args()

    if args.save_all:
        save_range(DCRFARM_START_DATE, date.today())
        return

    if args.month:
        mdate = datetime.strptime(args.month, "%Y%m").replace(tzinfo = timezone.utc)
    else:
        now = datetime.now(timezone.utc)
        mdate = dec_month(now)
        print("assuming month: " + mdate.strftime("%B %Y") + " (use -m to change)")

    start_date, end_date = month_interval(mdate)

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
        ua_stats, daily_mean_sum = calc_node_stats(dcrfarm_data)
        if args.print_counts:
            print_node_counts(ua_stats, daily_mean_sum)
        stats = calc_node_group_stats(ua_stats, daily_mean_sum)
        # print the stats in desired format.
        print_node_stats(stats, start_date)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error: " + str(e))
        sys.exit(1)
