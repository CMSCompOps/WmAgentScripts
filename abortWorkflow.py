#!/usr/bin/env python
import json, phedexSubscription
import urllib2,urllib, httplib, sys, re, os



def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:dbsTest workflowname"
		sys.exit(0)
	workflow=args[0]
	url='cmsweb.cern.ch'
	#phedexSubscription.setWorkflowRunning(url, workflow)
	phedexSubscription.abortWorkflow(url, workflow)
	sys.exit(0);

if __name__ == "__main__":
	main()

#Change 1 by Julian Badillo just to test GIT
