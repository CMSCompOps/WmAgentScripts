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

issues_types = ['highprio','dsstuck','mostlydone','veryold','subscribe','wronglfnbase','trstuck','acdc', 'unstaged']
live_status = ['assigned','acquired','running-open','running-closed','completed']
running_status = ['acquired','running','running-open','running-closed']
afs_base ='/afs/cern.ch/user/j/jbadillo/www/'
reprocdir = afs_base+'reproc'
mcdir = afs_base+'mc'
tcdir = afs_base+'tc'

#TODO calculate based on load
stuck_days = 3.0
old_days = 7.0


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

def getpriorities(s, campaign, zone, status, types):
    """
    Get the different priority values, in decreasing order
    filtered by campaign, zone and status
    """
    p = set()
    for r in s:
        if r['priority'] not in p:
            #matching filter
            if ((not campaign or campaign == r['campaign'] ) 
            and (not zone or zone == r['zone']) 
            and r['status'] in status
            and (not types or r['type' ] in types)):
                p.add(r['priority'])
    p = sorted(p)
    p.reverse()
    print p
    return p

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

    #print the ones that can be force-completed
    f.write('<h3>Force-complete (very old that are mostly done)</h3>')
    f.write('<pre>')
    #intersection between old and mostly done
    for i in sorted(issues['veryold'] & issues['mostlydone']):
        #ignore extensions
        if '_EXT_' in i:
            continue
        f.write('%s\n' % i)
    f.write('</pre>')
    
    #print the oldest ones
    f.write('<h3>Oldest requests in the system</h3>')
    f.write('<pre>')
    if oldest:
        days = sorted(oldest.keys(), reverse=True)[:3]
        for d in days:
            for req in sorted(oldest[d]):
                f.write('%s (%s days)\n' % (req, d))
    f.write('</pre>')

    #print the highest prio ones
    f.write('<h3>Highest priority</h3>')
    f.write('<pre>')
    for i in sorted(issues['highprio']):
        #ignore extensions
        if '_EXT_' in i:
            continue
        req = irlist[i]
        f.write('%s (%s)\n' % (i, human(req['priority'])))
    f.write('</pre>')
    
    #print the ones that need check
    f.write('<h3>Check needed</h3>')
    f.write('<pre>')
    for i in sorted(issues['wronglfnbase']):
        f.write('%s\n' % i)
    f.write('</pre>')

    #print the ones with no new events
    f.write('<h3>Datasets stuck (without new events)</h3>')
    f.write('<pre>')
    #sort by days stuck
    ls = [(r, irlist[r]['dayssame']) for r in issues['dsstuck']]
    for r, d in sorted(ls, key=lambda x: -x[1]):
        f.write('%s (%.2f days)\n' % (r, d))
    f.write('</pre>')
    
    #print the ones with no new events
    f.write('<h3>Input not subscribed to site</h3>')
    f.write('<pre>')
    for i in sorted(issues['unstaged']):
        req = irlist[i]
        f.write('%s (%s)\n' % (i, req['sites'][0]))
    f.write('</pre>')

    #the table
    f.write('<h3>Summary</h3>\n')
    #create headers
    h = ['request','priority','reqnumevts']
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
            elif i == 'priority':
                f.write('<td align=left>%s</td>' % (human(req['priority'])))
            elif i == 'reqnumevts':
                f.write('<td align=left>%s</td>' % (human(req['expectedevents'])))
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
    s = (','.join(["col_"+str(i+3)+":'select'" for i in range(len(issues_types))]))
    f.write('<script language="javascript" type="text/javascript">\n'
        'var issuestableFilters = {\n'
        'col_0: "none",\n'
        'col_1: "none",\n'
        'col_2: "none",\n'
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
        #TODO Ignore Dave's, Stefan's and Alan's tests
        if ('dmason' in r['requestname'] or 
            'TEST' in r['requestname'] or
            'piperov' in r['requestname'] or 
            'Backfill' in r['requestname']):
            continue
        s2.append(r)
    s = s2

    highest_prio = max( getpriorities(s,None,None,live_status, types)+[0])
    print "highest_prio", highest_prio
    #for status in ['assigned','acquired','running-open','running-closed','completed']:
    #   for priority in getpriorities(s,'','',[status]):
    for r in s:
        #calculate splitting
        if r['type'] == 'MonteCarlo':
            split = r['events_per_job']
        elif r['type'] == 'MonteCarloFromGEN':
            split = r['lumis_per_job']
        elif r['type'] == 'ReDigi':
            split = r['events_per_job']
        else:
            split = 0
        jlist=r['js']
        js = {}
        for j in jlist.keys():
            if j == 'sucess':
                k = 'success'
            else:
                k = j
            js[k] = jlist[j]
        #days that the output datasets have not been modified
        days = []
        #if status == r['status'] and priority == r['priority']:
        mostlydone = True if r['outputdatasetinfo'] else False
        for ods in r['outputdatasetinfo']:
            if 'custodialsites' in r and r['custodialsites']:
                custodialt1 = r['custodialsites'][0]
            etas = ''
            eta = 0
            eperc = 0
            if 'events' in ods.keys():
                try:
                    eperc = float(ods['events']) / r['expectedevents'] * 100
                except:
                    eperc = 0
                if eperc > 5 and r['status'] in running_status:
                    a=(datetime.datetime.now() - datetime.datetime.fromtimestamp(ods['createts']))
                    elapsedtime = (a.days*24*3600+a.seconds)/3600
                    if elapsedtime > 0:
                        if 'created' in js.keys() and js['created'] > 0 and js['success'] > 0 and js['running'] > 0 and js['running']/js['created'] > .1 and 0:
                            eta=(js['success']/elapsedtime)*(js['created']-js['success'])/js['running']
                        else:
                            speed = ods['events'] / elapsedtime
                            remainingevents = r['expectedevents'] - ods['events']
                            if speed>0:
                                eta = remainingevents / speed
                            else:
                                eta = 0
                        
                        if eta < 0:
                            eta = 0
                        #print "elapsedtime = %s ods['events'] = %s ods['createts'] = %s speed = %s remainingevents = %s eta = %s" % (elapsedtime,ods['events'],ods['createts'],speed,remainingevents,eta)
                    else:
                        eta = 0
                    if eta == 0:
                        etas = ''
                    elif eta > 24*30:
                        etas = '&#8734;'
                    else:
                        etas = '%sd' % (eta/24+1)
            phperc = ''
            transferred_events = 0
            if 'phtrinfo' in ods.keys():
                for p in ods['phtrinfo']:
                    if p['custodial'] == 'y':
                        if 'perc' in p.keys():
                            phperc = "%s" % p['perc']
                            transferred_events = int(float(ods['events']) * float(phperc) / 100)
                        break
            if 'created' in js.keys() and js['created'] > 0:
                jobperc = min(100,100*float(js['success']+js['failure'])/js['created'])
            else:
                jobperc = 0
            if eperc > 20 and 'phreqinfo' in ods.keys() and ods['phreqinfo'] == {}:
                issues['subscribe'].add(r['requestname'])
            #if it's pointing to the wrong LFN

           #TODO LFN for data?


            if (r['status'] in live_status and
                (('HIN' in r['requestname'] and r['mergedLFNBase'] != '/store/himc' ) 
                or (r['outputdatasetinfo'][0]['name'][-3:] == 'GEN' and r['mergedLFNBase'] != '/store/generator')
                or (r['outputdatasetinfo'][0]['name'][-3:] in ['GEN-SIM','AODSIM'] and r['mergedLFNBase'] != '/store/mc'))):
                issues['wronglfnbase'].add(r['requestname'])
            if 'acdc' in r and len(r['acdc']) > 1:
                issues['acdc'].add(r['requestname'])
                #alarm = 'ACDC(%s)' % len(r['acdc'])
                alarmlink='https://cmst2.web.cern.ch/cmst2/mc/requests/%s.html' % r['requestname']
            if r['status'] in ['completed','closed-out'] and 'phtrinfo' in ods.keys():
                j = {}
                for i in ods['phtrinfo']:
                    if 'custodial' in i.keys():
                        if i['custodial'] == 'y':
                            j = i
                            break
                if 'perc' in j.keys():
                    if j['perc'] < 100:
                        issues['trstuck'].add(r['requestname'])
                        #alarmlink = 'https://cmsweb.cern.ch/phedex/prod/Activity::ErrorInfo?tofilter=%s*&fromfilter=.*&report_code=.*&xfer_code=.*&to_pfn=.*%%2Fstore%%2Fmc%%2F%s%%2F%s%%2F.*&from_pfn=.*%%2Fstore%%2Fmc%%2F%s%%2F%s%%2F.*&log_detail=.*&log_validate=.*&.submit=Update#' % (custodialt1,r['acquisitionEra'],r['primaryds'],r['acquisitionEra'],r['primaryds'])
            elif r['status'] in ['acquired','running','running-open','running-closed'] and 'cooloff' in js.keys() and js['cooloff'] > 100:
                pass
                #issues['cooloff'].add(r['requestname'])
                #alarmlink='' % ()
            #all datasets ready
            if 'DQMIO' in ods['name']:
                mostlydone &= True
            elif(eperc >=95 and not 'SMS' in r['outputdatasetinfo'][0]['name'] 
                    and 'CMSSM' not in r['outputdatasetinfo'][0]['name'] 
                    and r['status'] in ['acquired','running-open','running-closed']):
                mostlydone &= True
            else:
                mostlydone = False
            
            #only higher requests that are in live status
            if r['priority'] == highest_prio and r['status'] in live_status:
                issues['highprio'].add(r['requestname'])
            
            #get the oldest requests in the system
            if r['status'] in live_status and r['requestdays'] > old_days+int(r['expectedevents']/10000000):
                issues['veryold'].add(r['requestname'])
                daysold = int(r['requestdays'])
                if daysold not in oldest.keys():
                    oldest[daysold] = set()
                oldest[daysold].add(r['requestname'])
                #alarmlink = ''
            
            if r['requestname'] in urgent_requests:
                color = '#CCCCFF'
                ##issues['urgent'].add(r['requestname'])
            else:
                color = '#FFFFFF'
            if r['status'] in ['completed']:
                temp_wsum = ",<a href='https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s' target='_blank'>sum</a>" % r['requestname']
            else:
                temp_wsum = ''

            #get the days that the dataset had no new events
            if 'lastmodts' not in ods or not ods['lastmodts']:
                days.append(0)
            else:
                lastmodts = datetime.datetime.fromtimestamp(ods['lastmodts'])
                delta = now - lastmodts
                days.append(delta.days + delta.seconds/3600.0/24.0)
                #alarmlink='https://cmsweb.cern.ch/das/request?view=list&limit=10&instance=cms_dbs_prod_global&input=dataset+dataset=%s*+' % ods['name']

            #acquired and unsubscribed input
            if r['status'] == 'acquired':
                #get one site
                if len(r['sites']) == 1:
                    site = r['sites'][0]
                    if 'phtrinfo' in r['inputdatasetinfo']:
                        nodes_avail = [str(subs['node']) for subs in r['inputdatasetinfo']['phtrinfo']]
                    else:
                        nodes_avail = []
                    #is acquired on a site in which the input is not subscribed
                    if( not nodes_avail
                        or( site not in nodes_avail and
                            site+'_Disk' not in nodes_avail)):
                        print "unstaged"
                        issues['unstaged'].add(r['requestname'])
        #if all datasets over 95%
        if mostlydone:
            issues['mostlydone'].add(r['requestname'])
                         
        #get the min days without new events
        dayssame = min(days) if days else 0
        r['dayssame'] = dayssame
        #check dataset stuck
        if r['status'] in running_status and dayssame > stuck_days:
            issues['dsstuck'].add(r['requestname'])


def main():

    #read json files
    now = datetime.datetime.now()
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/data.json' % afs_base)
    datajsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/data.json' % afs_base).read()
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

    #generate for MonteCarlo
    #empty urgent requests
    urgent_requestsMC = []
    oldestMC = {}
    issuesMC = {}
    #process all the requests and obtain issues
    makeissuessummary(s, issuesMC, oldestMC, urgent_requestsMC, ['MonteCarlo', 'MonteCarloFromGEN'])
    #write the output in html format
    writehtml(issuesMC, oldestMC, s, now, datajsonmtime, wrjsonmtime, dbsjsonmtime, pledgedjsonmtime, mcdir, "MonteCarlo" )

    #generate for ReDigi
    #empty urgent requests
    urgent_requestsRD = []
    oldestRD = {}
    issuesRD = {}
    #process all the requests and obtain issues
    makeissuessummary(s, issuesRD, oldestRD, urgent_requestsRD, ['ReDigi'])
    #write the output in html format
    writehtml(issuesRD, oldestRD, s, now, datajsonmtime, wrjsonmtime, dbsjsonmtime, pledgedjsonmtime, reprocdir, "ReDigi" )

    #generate for TaskChains
    #empty urgent requests
    urgent_requestsTC = []
    oldestTC = {}
    issuesTC = {}
    #process all the requests and obtain issues
    makeissuessummary(s, issuesTC, oldestTC, urgent_requestsTC, ['TaskChain'])
    #write the output in html format
    writehtml(issuesTC, oldestTC, s, now, datajsonmtime, wrjsonmtime, dbsjsonmtime, pledgedjsonmtime, tcdir, "TaskChain" )
    


    # manage acquired/running,completed,closed-out requests;purge them at every cycle
    sys.exit(0)

if __name__ == "__main__":
        main()
