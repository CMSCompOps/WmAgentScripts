#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, phedexSubscription
import json
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import dbsTest
import optparse

def main():
    parser = optparse.OptionParser()
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
        sys.exit(0)
        
    #args=sys.argv[1:]
    if not len(args)==1:
        print "usage: python getRelValDsetNames.py <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)
    inputFile=args[0]
    f = open(inputFile, 'r')

    url='cmsweb.cern.ch'
    for line in f:
        workflow = line.rstrip('\n')
        outputDataSets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
        #print "These are the output datasets:"
        #print outputDataSets
        #inputEvents=getInputEvents(url, workflow)
        #print inputEvents
        for dataset in outputDataSets:
            outputEvents=dbsTest.getEventCountDataSet("https://cmsweb.cern.ch",dataset)
    #        print dataset+" match: "+str(outputEvents/float(inputEvents)*100) +"%"
            print dataset+"\t\t"+str(outputEvents)

    f.close
    sys.exit(0);

if __name__ == "__main__":
    main()

