#!/usr/bin/env python -u
import urllib2,urllib, httplib, sys, re, os
import json
import optparse
from xml.dom import minidom


parser = optparse.OptionParser()
parser.add_option('--site', dest='site')
parser.add_option('--dataset', dest='dataset')

(options,args) = parser.parse_args()

if options.site == None or options.dataset == None:
    print "Usage: python2.6 check_if_dataset_is_at_a_site.py --site SITENAME --dataset DATASETNAME"
    sys.exit(0)


site=options.site
dataset = options.dataset


#block=["/DoubleMuParked/Run2012D-v1/RAW%23f469d5be-3a9e-11e2-8e2f-842b2b4671d8"



#os.popen("export PYTHON_PATH=/home/relval/WMCore/src/python/")

#os.environ['PYTHONPATH'] = '/home/relval/WMCore/src/python/'
#os.environ['PATH'] = '/home/relval/scripts:/usr/sue/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin'
#os.environ['LD_LIBRARY_PATH'] = ""
#os.putenv('LD_LIBRARY_PATH','')

#print "hi"

#if len(sys.argv) == 1:
#    os.system('python2.6 delete_this.py oo')

#print "hi2"


subscribed_to_disk=False

url=('https://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscriptions?dataset='+dataset)
urlinput = urllib.urlopen(url)
#print url
#print urlinput
xmldoc = minidom.parse(urlinput)
for phedex in  xmldoc.childNodes:
    for dataset in phedex.childNodes:
        for subscription in dataset.childNodes:
            if subscription.attributes['node'].value == site:
                subscribed_to_disk=True

if subscribed_to_disk:
    sys.exit(1)
else:
    sys.exit(0)
