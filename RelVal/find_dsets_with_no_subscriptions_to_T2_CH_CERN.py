#!/usr/bin/env python

import sys,getopt
import urllib
from xml.dom import minidom

f = open("delete_this.txt", 'r')
#f = open("delete_this.txt","r")

for line in f:
    dset = line.rstrip('\n')
    subscribed_to_t2_ch_cern = False
    url1='https://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscriptions?dataset=' + dset
    url2='https://cmsweb.cern.ch/phedex/datasvc/xml/prod/data?dataset='+dset
    input1 = urllib.urlopen(url1)
    xmldoc1 = minidom.parse(input1)
    input2 = urllib.urlopen(url2)
    xmldoc2 = minidom.parse(input2)

    #make sure the dataset is not subscribed to T2_CH_CERN
    for phedex in  xmldoc1.childNodes:
        for dataset in phedex.childNodes:
            for subscription in dataset.childNodes:

                if subscription.attributes['node'].value == "T2_CH_CERN":
                    subscribed_to_t2_ch_cern = True
                
                #if (subscription.attributes['group'].value == "RelVal" or subscription.attributes['group'].value == "DataOps") and subscription.attributes['level'].value == "DATASET" and subscription.attributes['percent_files'].value == "100" and subscription.attributes['percent_bytes'].value == "100":
                #print str(subscription.attributes['node'].value) + " "+dset
                        
                #print subscription.attributes['group'].value
                #print subscription.attributes['node'].value
                #print subscription.attributes['level'].value
                #print subscription.attributes['percent_bytes'].value
                #print subscription.attributes['percent_files'].value

    #make sure no blocks in the dataset are subscribed to T2_CH_CERN            
    for phedex in  xmldoc2.childNodes:
        for dbs in phedex.childNodes:
            for dataset in dbs.childNodes:
                for block in dataset.childNodes:
                    url3=('https://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscriptions?block='+block.attributes['name'].value).replace('#','%23')
                    input3 = urllib.urlopen(url3)
                    xmldoc3 = minidom.parse(input3)
                    for phedex in  xmldoc3.childNodes:
                        for dataset in phedex.childNodes:
                            for block in dataset.childNodes:
                                for subscription in block.childNodes:
                                    if subscription.attributes['node'].value == "T2_CH_CERN":
                                        subscribed_to_t2_ch_cern = True
                                    
    if not subscribed_to_t2_ch_cern:
        print dset
