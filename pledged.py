#!/usr/bin/env python

import urllib2, urllib
from xml.dom import minidom

def loadnamingconvention():
	sitelist = {}
	url = 'https://cmsweb.cern.ch/sitedb/reports/showXMLReport/?reportid=naming_convention.ini'
	naming = minidom.parse(urllib.urlopen(url))
	for node in naming.getElementsByTagName('item'):
		# the http call doesn't work with T3*
		tname = node.getElementsByTagName('cms')[0].firstChild.data
		if "T2_" in tname or ( "T1_" in tname and "_Disk" not in tname):
			if tname != "T2_CH_CAF" and tname != 'T1_CH_CERN':
				sitelist[node.getElementsByTagName('id')[0].firstChild.data] = tname
	return sitelist

def getslots(index):
	url = 'https://cmsweb.cern.ch/sitedb/json/index/Pledge?site=%s' % index
	data = urllib2.urlopen(url)
	s = data.read()
	try:
		res = eval(s)
		return int(res['0']['job_slots - #'])
	except:
		return 0

def allpledged():
	sitelist = {}
	pledged = {}
	sites = loadnamingconvention()
	for (k,v) in sites.items():
		slots = getslots(k)
		if slots > 0:
		#if 1:
			pledged[v] = slots
	return pledged

p = allpledged()

for (k,v) in sorted(p.iteritems()):
	print "%s %s" % (k,v)
