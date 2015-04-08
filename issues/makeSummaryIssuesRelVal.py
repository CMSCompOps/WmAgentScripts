#!/usr/bin/env python
"""
    Creates a HTML summary of the ReDigi's issues
"""

import sys,urllib,urllib2,re,time,os
try:
    import json
except ImportError:
    import simplejson as json
import optparse
import httplib
import datetime
import time
import zlib 

# TODO see local_queue
#TODO: view for distribution of all the Summer12 VALID GEN-SIM not yet chained to any DR step
#TODO: assignment-approved 1week, input datasets slots ready
# TODO MC statistics (DBS)
# TODO sites/pledged
# TODO add cooloffs/failures (new page?)

header = ('<html>\n'
        '<head>\n'
        '<meta http-equiv="Refresh" content="1800">\n'
        '<link rel="stylesheet" type="text/css" href="../style.css" />\n'
        '<script language="javascript" type="text/javascript" src="../actb.js"></script><!-- External script -->\n'
        '<script language="javascript" type="text/javascript" src="../tablefilter.js"></script>\n'
        '<title>%s Status - Issues</title>\n'
        '<meta http-equiv="Refresh" content="1800">\n'
        '</head>\n'
        '<body>\n')

bar = ('<table><tr>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/">Summary</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/requests.html">Requests</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/assignment.html">Assignment</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/closed-out.html">Closed-out</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/announce.html">Announce batches</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/sites.html">Sites</a></td>'
    '<td><a href="http://cmst2.web.cern.ch/cmst2/mc/issues.html">Issues</a></td>'
    '<td><a target="_blank" href="https://cmst2.web.cern.ch/cmst2/ops.php">Ops</a></td>'
    '<td><a target="_blank" href="https://cmsweb.cern.ch/wmstats/index.html">WMStats</a></td>'
    '<td><a target="_blank" href="http://cmst2.web.cern.ch/cmst2/mc/announcements.log">Announcement logs</a></td>'
    '<td><a target="_blank" href="http://www.gridpp.rl.ac.uk/cms/reprocessingcampaigns_totals.html">Processing campaigns</a></td>'
    '<td><a target="_blank" href="https://hypernews.cern.ch/HyperNews/CMS/SECURED/edit-response.pl/datasets.html">New announcement HN</a></td>'
    '<td><a target="_blank" href="https://savannah.cern.ch/support/?group=cmscompinfrasup&func=additem">New Savannah</a></td>'
    '</tr></table><hr>\n')

foot = ("<hr><i>Last update: %s</i><br/>\n"
        "<i>GEN-SIM Waiting room: <a target='_blank' href='http://cmst2.web.cern.ch/cmst2/mc/gen-sim-wr.json'>gen-sim-wr.json</a> (updated: %s)</i><br/><i>Acquired->Closed-out MonteCarlo* requests JSON file: <a target='_blank' href='http://cmst2.web.cern.ch/cmst2/mc/data.json'>data.json</a> (updated: %s)</i><br/><i>Pledged JSON file: <a target='_blank' href='http://cmst2.web.cern.ch/cmst2/mc/pledged.json'>pledged.json</a> (updated: %s)</i><br/><i>Assignment txt file: <a target='_blank' href='http://cmst2.web.cern.ch/cmst2/mc/assignment.txt'>\n"
        "assignment.txt</a></i><br/>\n"
        "</body>\n"
        "</html>\n")

issues_types = ['veryold','acquired','assigned', 'failed' ,'nojobs']
live_status = ['assigned','acquired','failed','running-open','running-closed']
running_status = ['acquired','running','running-open','running-closed']
afs_base ='/afs/cern.ch/user/j/jbadillo/www/'
takschaindir = afs_base+'taskchain'

#TODO calculate based on load
stuck_days = 1
old_days = 3


def human(n):
    """
    Format a number in a easy reading way
    """
    if n<1000:
        return "%s" % n
    elif n>=1000 and n<1000000:
        order = 1
    elif n>=1000000 and n<1000000000:
        order = 2
    else:
        order = 3

    norm = pow(10,3*order)

    value = float(n)/norm

    letter = {1:'k',2:'M',3:'G'}
    return ("%.1f%s" % (value,letter[order])).replace(".0", "")

def loadcampaignconfig(f):
    try:
        d = open(f).read()
    except:
        print "Cannot load config file %s" % f
        sys.exit(1)
    try:
        s = eval(d.strip())
        if '' in s:
            print "XXXXXXXXX"
            s.remove('')
    except:
        print "Cannot eval config file %s " % f
        sys.exit(1)
    print "\nConfiguration loaded successfully from %s" % f
    #for c in s.keys():
    #       print "%s" % c
    #       for i in s[c]:
    #               print "\t%s\t%s" % (i,s[c][i])
    return s

def writehtml(issues,  oldest, s, now, datajsonmtime, wrjsonmtime, dbsjsonmtime, pledgedjsonmtime, folder, title):
    """
    Write the HTML page
    """
    f = open('%s/issues.html' % folder,'w')
    f.write(header%title)
    f.write(bar)

    irlist = {}
    #create a dictionary with the requests that have at least one isse
    for r in s:
        name = r['requestname']
        #if is at least in one issue set
        for issue in issues.values():
            if name in issue:
                irlist[name] = r
                break

    f.write('<h3>Total Relvals: %s</h3>'%len(s))

    #print the oldest ones
    f.write('<h3>Oldest requests in the system</h3>')
    f.write('<pre>')
    if oldest:
        days = max(oldest.keys())
        for req in sorted(oldest[days]):
            f.write('%s (%s days)\n' % (req, days))
    f.write('</pre>')

    #the table
    f.write('<h3>Summary</h3>\n')
    #create headers
    h = ['request','jobs','days']
    for i in issues.keys():
        h.append(i)

    f.write("<table border=1 id='issuestable'>\n")
    f.write("<tr>")
    for i in h:
        f.write('<th>%s</th>' % i)
    f.write("</tr>")

    for rname, req in sorted(irlist.items()):
        f.write("<tr onMouseOver=\"this.bgColor='#DDDDDD'\" onMouseOut=\"this.bgColor='#FFFFFF'\">\n")
        for i in h:
            if i == 'request':
                f.write('<td><a target="_blank" href="https://cmsweb.cern.ch/reqmgr/view/details/%s">%s</a></td>\n' % (rname,rname))
            elif i == 'jobs':
                f.write('<td align=left>%s</td>' % req['jobs'])
            elif i == 'days':
                f.write('<td align=left>%s</td>' % req['requestdays'])
            else:
                if rname in issues[i]:
                    color='#FF0000'
                    stri='<strong>X</strong>'
                else:
                    color='#00FF00'
                    stri='&nbsp;'
                #f.write('<td align=center bgcolor=%s>&nbsp;</td>' % color)
                f.write('<td align=center>%s</td>\n' % stri)
        f.write("</tr>\n")
    f.write("</table>\n")
    #script for filtering rows
    s = (','.join(["col_"+str(i+2)+":'select'" for i in range(len(issues_types)+1)]))
    f.write('<script language="javascript" type="text/javascript">\n'
        'var issuestableFilters = {\n'
        'col_0: "none",\n'
        'col_1: "none",\n'
        + s +
        '};\n'
        'setFilterGrid("issuestable",0,issuestableFilters);\n'
        '</script>\n')
    f.write(foot%(str(now),wrjsonmtime,datajsonmtime,pledgedjsonmtime))
    f.close()


def makeissuessummary(s, issues, oldest, urgent_requests, types):
    """
    processes the requests list and creates an issues dictionary
    """
    now = datetime.datetime.now()

    for i in issues_types:
        issues[i] = set()
    
    s2 = [] 
    
    #filter before
    for r in s:
        #only selected type
        if r['type'] not in types:
            continue
        #only the ones that have RVCMSSW in request name
        if 'RVCMSSW' not in r['requestname']:
            continue
        s2.append(r)
    s = s2
        
    
    #for status in ['assigned','acquired','running-open','running-closed','completed']:
    #   for priority in getpriorities(s,'','',[status]):
    for r in s:
        #job summary
        jlist=r['js']
        js = {}
        for j in jlist.keys():
            if j == 'sucess':
                k = 'success'
            else:
                k = j
            js[k] = jlist[j]
        #Total of created jobs to display
        if 'created' in js:
            r['jobs'] = js['created']
        else:
            r['jobs'] = 0    
        #is in assigned or acquired or failed
        if r['requestdays'] > 0 and r['status'] == 'acquired':
            issues['acquired'].add(r['requestname'])

        if r['status'] == 'assigned':
            issues['assigned'].add(r['requestname'])

        if r['status'] == 'failed':
            issues['failed'].add(r['requestname'])

        if r['status'] in running_status:
            #is running but has no running jobs
            if js and ( js['running'] == 0 and js['pending'] == 0 ):
                issues['nojobs'].add(r['requestname'])

        #days old
        if r['status'] in live_status and r['requestdays'] >= old_days:
            issues['veryold'].add(r['requestname'])
            if r['requestdays'] not in oldest.keys():
                oldest[r['requestdays']] = set()
            oldest[r['requestdays']].add(r['requestname'])           
        
def main():

    #read json files
    now = datetime.datetime.now()
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/data_taskchain.json' % afs_base)
    datajsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/data_taskchain.json' % afs_base).read()
    s = json.loads(d)

    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/gen-sim-wr.json' % afs_base)
    wrjsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/gen-sim-wr.json' % afs_base).read()
    wrjson = json.loads(d)

    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/dbs.json' % afs_base)
    dbsjsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/dbs.json' % afs_base).read()
    dbsjson = json.loads(d)

    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/pledged.json' % afs_base)
    pledgedjsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/pledged.json' % afs_base).read()
    pledged = json.loads(d)
    totalpledged = 0

    #calculate total pledge
    for i in pledged.keys():
        totalpledged = totalpledged + pledged[i]

    #generate for RelVals
    #empty urgent requests
    urgent_requestsRelVal = []
    oldestRelVal = {}
    issuesRelVal = {}
    #process all the requests and obtain issues
    makeissuessummary(s, issuesRelVal, oldestRelVal, urgent_requestsRelVal, ['TaskChain'])
    #write the output in html format
    writehtml(issuesRelVal, oldestRelVal, s, now, datajsonmtime, wrjsonmtime, dbsjsonmtime, pledgedjsonmtime, takschaindir, "RelVal" )

    # manage acquired/running,completed,closed-out requests;purge them at every cycle
    sys.exit(0)

if __name__ == "__main__":
        main()
