#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import optparse

def assignRequest(url,workflow,team,site,era,procversion, activity, lfn):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": "/store/backfill/1",
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
	      "maxRSS": 4294967296,
              "maxVSize": 4294967296,
              "AcquisitionEra": era,
	      "dashboard": activity,
              "ProcessingVersion": procversion,
              "checkbox"+workflow: "checked"}

    encodedParams = urllib.urlencode(params, True)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
  	sys.exit(1)
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'with processing version',procversion
    return



def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='Workflow Name',dest='workflow')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Site',dest='site')
	parser.add_option('-e', '--era', help='Acquistion era',dest='era')
	parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
	parser.add_option('-a', '--activity', help='Dashboard Activity',dest='activity')
	parser.add_option('-l', '--lfn', help='Merged LFN base',dest='lfn')
	(options,args) = parser.parse_args()
	if not options.workflow:
		print "Write a workflow is required"
		sys.exit(0);
	if not options.team:
		print "Write a team"
		sys.exit(0);
	if not options.site:
		print "Write a Site"
		sys.exit(0);
	if not options.era:
		print "Write an Acquision era"
		sys.exit(0);
	if not options.procversion:
		print "Write a Processing Version"
		sys.exit(0);
	activity='reprocessing'
	if not options.activity:
		activity='reprocessing'
	else:
		activity=options.activity
	lfn='/store/backfill/1'
	if not options.lfn:
		lfn='/store/backfill/1'
	else:
		lfn=options.lfn
	workflow=options.workflow
	team=options.team
	site=options.site
	era=options.era
	procversion=options.procversion
	assignRequest(url,workflow,team,site,era,procversion, activity, lfn)
	sys.exit(0);

if __name__ == "__main__":
	main()
