#!/usr/bin/env python
import sys
import os
try:
    import json
except ImportError:
    import simplejson as json
import datetime
import time

afs_base = '/afs/cern.ch/user/j/jbadillo/www/'
live_status = ['assigned', 'acquired',
               'running-open', 'running-closed', 'completed']

head = ('<html>'
        '<head>'
        '<link rel="stylesheet" type="text/css" href="style.css" />'
        '</head>'
        '<body>')

foot = ('<p>Last update: %s CERN time</p>'
        '</body>'
        '</html>')


def human(n):
    """
    Format a number in a easy reading way
    """
    if n < 1000:
        return "%s" % n
    elif n >= 1000 and n < 1000000:
        order = 1
    elif n >= 1000000 and n < 1000000000:
        order = 2
    else:
        order = 3

    norm = pow(10, 3 * order)

    value = float(n) / norm

    letter = {1: 'k', 2: 'M', 3: 'G'}
    return ("%.1f%s" % (value, letter[order])).replace(".0", "")


def writehtml(s, t, r):
    """
    Write the HTML page
    """
    f = open('%s/stuckness.html' % afs_base, 'w')
    f.write(head)
    f.write('<h3>Stuckness Summary</h3>')
    f.write('<table><tr><td>')
    f.write('days = %s<br>' % t)
    f.write('reqs = %s<br>' % r)
    avg = t / r
    threshold = 1.5 * avg
    f.write('avg = %s<br>' % avg)
    f.write('threshold = %s<br>' % (threshold))
    f.write('</td><td> <img src="stuckness.png"></td></tr></table>')
    h = ['request', 'status', 'priority', 'relativeprio',
         'dayssame', 'assignedon', 'stuckness']

    f.write("<table border=1>")
    f.write("<tr>")
    f.write("<th></th>")
    for i in h:
        f.write("<th>%s</th>" % i)
    f.write("</tr>")
    i = 1
    for req in s:
        # print only the ones over the threshold of stuckness
        if req['dayssame'] < threshold:
            continue
        f.write("<tr>")
        f.write('<td>%s</td>' % i)
        i += 1
        f.write('<td><a target="_blank" href="https://cmsweb.cern.ch/reqmgr/view/details/%s.html">%s</a></td>' %
                (req['requestname'], req['requestname']))
        f.write('<td>%s</td>' % req['status'])
        f.write('<td>%s</td>' % human(req['priority']))
        f.write('<td>%.3f</td>' % req['relativeprio'])
        f.write('<td>%.2f</td>' % req['dayssame'])
        f.write('<td>%s</td>' % req['update'].split()[0])
        f.write('<td>%s</td>' % req['stuckness'])
    f.write("</tr>")
    f.write("</table>")
    f.write(foot % time.strftime("%c"))
    f.close()


def getpriorities(s, campaign, zone, status):
    """
    Get the different priority values, in decreasing order
    filtered by campaign, zone and status
    """
    p = set()
    for r in s:
        if r['priority'] not in p:
            if (not campaign or campaign == getcampaign(r)) and (not zone or zone == r['zone']) and r['status'] in status:
                p.add(r['priority'])
    p = sorted(p)
    p.reverse()
    return p


def main():

    # read json files
    now = datetime.datetime.now()
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(
        '%s/data.json' % afs_base)
    datajsonmtime = "%s" % time.ctime(mtime)
    d = open('%s/data.json' % afs_base).read()
    s = json.loads(d)

    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(
        '%s/pledged.json' % afs_base)
    pledgedjsonmtime = "%s" % time.ctime(mtime)
    d = open('%s/pledged.json' % afs_base).read()
    pledged = json.loads(d)
    totalpledged = 0
    # calculate total pledge
    for i in pledged.keys():
        totalpledged = totalpledged + pledged[i]

    urgent_requests = []
    stuck_days = 5
    old_days = 15
    oldest = {}

    s2 = []
    # filter out test stuff
    for r in s:
        if r['status'] not in live_status:
            continue
        name = r['requestname']
        status = r['status']
        campaign = r['campaign']
        rtype = r['type']
        # ignore anything that says "Test"
        if 'test' in name.lower() or 'test' in campaign.lower():
            continue
        # ignore anything that says "backfill"
        if 'backfill' in name.lower() or 'backfill' in campaign.lower():
            continue
        # ignore resubmissions:
        if rtype == 'Resubmission':
            continue
        # ignore dave mason's tests and stefan's tests
        if 'dmason' in name or 'piperov' in name:
            continue
        # ignore relvals and test
        if 'dmason' in name or 'piperov' in name:
            continue
        # ignore relvals
        if 'RVCMSSW' in name:
            continue
        s2.append(r)
    s = s2

    highest_prio = max(getpriorities(s, None, None, live_status))
    # for status in ['assigned','acquired','running-open','running-closed','completed']:
    #   for priority in getpriorities(s,'','',[status]):
    totaldays = 0
    reqs = 0
    for r in s:
        name = r['requestname']
        status = r['status']
        campaign = r['campaign']
        rtype = r['type']
        days = []
        for ods in r['outputdatasetinfo']:
            if 'lastmodts' not in ods or not ods['lastmodts']:
                days.append(0)
            else:
                lastmodts = datetime.datetime.fromtimestamp(ods['lastmodts'])
                delta = now - lastmodts
                days.append(delta.days + delta.seconds / 3600.0 / 24.0)
        dayssame = min(days) if days else 0
        # TODO calculate stuckness
        relativeprio = float(r['priority']) / highest_prio
        stuckness = relativeprio * dayssame
        r['relativeprio'] = relativeprio
        r['stuckness'] = stuckness
        r['dayssame'] = dayssame
        if dayssame:
            totaldays += dayssame
            reqs += 1

    # sort by stuckness
    s = sorted(s, reverse=True, key=lambda r: r[
               'stuckness'] if 'stuckness' in r else -1)
    print totaldays, reqs
    # write the output in html format
    writehtml(s, totaldays, reqs)

    # manage acquired/running,completed,closed-out requests;purge them at
    # every cycle
    sys.exit(0)

if __name__ == "__main__":
    main()
