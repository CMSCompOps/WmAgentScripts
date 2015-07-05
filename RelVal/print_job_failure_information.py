#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import time


def explain_failure(exitcode,failure):
    if exitcode == 61300:
        return "due to the hard timeout"
    elif exitcode == 50660:
        return "due to RSS"
    elif exitcode == 50664:
        return "due to running too long"
    elif exitcode == 134:
        return "due to a segmentation fault"
    elif exitcode == '8021':
        return "due to FileReadErrors"
    elif exitcode == '8028':
        return "due to FallbackFileOpenErrors"
    elif failure['details'] != None and 'Adding last ten lines of CMSSW stdout:' not in failure['details']:
        return "due to \n\n"+failure['details']+"\n"
    else:    
        return "due to exit code "+str(exitcode)
    
def provide_log_files(exitcode):
    if exitcode == '8028':
        return False
    elif exitcode == '8021':
        return False
    else:
        return True

url='cmsweb.cern.ch'
