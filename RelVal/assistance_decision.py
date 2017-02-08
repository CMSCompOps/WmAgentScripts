#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import time

assistance_exit_codes = ['8021','8028',71304]

default_threshold = 0.05

merge_threshold=0.1

harvesting_threshold=0.5

def assistance_decision(job_failure_information):

    assistance=False

    for wf in job_failure_information:

        firsttime=True
        for task in wf['task_dict']:

            if 'CleanupUnmerged' in task['task_name'] or 'LogCollect' in task['task_name']:
                continue
            if task['nfailurestot'] == 0:
                continue

            sum = 0

            for exitcode in assistance_exit_codes:

                if exitcode in task['failures'].keys():
                    sum+=task['failures'][exitcode]['number']

            if "HarvestMerged" in task['task_name']:
                if float(sum) / task['totaljobs'] > harvesting_threshold:
                    assistance=True

            elif "Merge" in task['task_name']:
                if float(sum) / task['totaljobs'] > merge_threshold:
                    assistance=True

            else:
                if float(sum) / task['totaljobs'] > default_threshold:
                    assistance=True


    return assistance
