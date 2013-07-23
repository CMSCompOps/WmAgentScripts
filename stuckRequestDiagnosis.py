"""
__reqmgr.CheckRequest__

Created on Jun 25, 2013

@author: dballest
"""

import logging
import os
import sys
import threading

from WMCore.Database.CMSCouch import Database
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMInit import connectToDB

def checkWorkQueue(requestName):
    result = {'ActiveAgents' : {},
              'ElementsRunning' : 0,
              'ElementsAcquired' : 0,
              'ElementsAvailable' : 0,
              'ElementsDone' : 0}
    x = Database('workqueue', 'https://cmsweb.cern.ch/couchdb')
    y = x.loadView('WorkQueue', 'elementsByParent', {'include_docs' : True}, [requestName])
    for entry in y['rows']:
        doc = entry['doc']
        element = doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
        status = element['Status']
        if status == 'Running':
            result['ElementsRunning'] += 1
        elif status == 'Acquired':
            result['ElementsAcquired'] += 1
        elif status == 'Available':
            result['ElementsAvailable'] += 1
        elif status == 'Done':
            result['ElementsDone'] += 1
        if status not in ['Done', 'Available']:
            agent = element['ChildQueueUrl']
            if agent not in result['ActiveAgents']:
                result['ActiveAgents'][agent] = 0
            result['ActiveAgents'][agent] += 1
    return result


def checkJobCountsAgent(requestName):
    os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)
    unfinishedTasks = formatter.formatDict(myThread.dbi.processData("""SELECT wmbs_workflow.task, wmbs_job_state.name,
                                                                              COUNT(wmbs_job.id) AS jobcount
                                                                     FROM wmbs_workflow
                                                                     INNER JOIN wmbs_subscription ON
                                                                         wmbs_subscription.workflow = wmbs_workflow.id
                                                                     INNER JOIN wmbs_jobgroup ON
                                                                         wmbs_jobgroup.subscription = wmbs_subscription.id
                                                                     INNER JOIN wmbs_job ON
                                                                         wmbs_job.jobgroup = wmbs_jobgroup.id
                                                                     INNER JOIN wmbs_job_state ON
                                                                         wmbs_job.state = wmbs_job_state.id
                                                                     WHERE wmbs_workflow.name = '%s' AND
                                                                           wmbs_subscription.finished = 0 AND
                                                                           wmbs_job_state.name != 'cleanout'
                                                                     GROUP BY wmbs_workflow.task,
                                                                              wmbs_job_state.name""" % requestName))
    result = {}
    for row in unfinishedTasks:
        if row['task'] not in result:
            result[row['task']] = {}
        result[row['task']][row['name']] = row['jobcount']
    for task in result:
        msg = "Task %s has " % task
        for state in result[task]:
            msg += '%d jobs %s ' % (result[task][state], state)
        print msg

    if not result:
        print "Check #1 failed, there are no unfinished tasks in the system apparently."
    else:
        return

    unfinishedSubs = formatter.formatDict(myThread.dbi.processData("""SELECT wmbs_subscription.id,
                                                                             wmbs_workflow.task
                                                                       FROM wmbs_workflow
                                                                       INNER JOIN wmbs_subscription ON
                                                                           wmbs_subscription.workflow = wmbs_workflow.id
                                                                       WHERE wmbs_workflow.name = '%s' AND
                                                                           wmbs_subscription.finished = 0""" % requestName))
    totalSubs = formatter.formatDict(myThread.dbi.processData("""SELECT wmbs_subscription.id,
                                                                             wmbs_workflow.task
                                                                       FROM wmbs_workflow
                                                                       INNER JOIN wmbs_subscription ON
                                                                           wmbs_subscription.workflow = wmbs_workflow.id
                                                                       WHERE wmbs_workflow.name = '%s'""" % requestName))
    print "There are %d subscriptions for this workflow, %d are incomplete." % (len(totalSubs), len(unfinishedSubs))
    if len(unfinishedSubs) != 0:
        print "It appears no jobs have been created for some unfinished subscriptions, check the health of the JobCreator or contact a developer."
    print "This workflow has all subscriptions as finished, the TaskArchiver should be eating through it now. This can take time though."
    return

def main():
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    if arg1 == 'agent':
        checkJobCountsAgent(arg2)
    else:
        listOpen = open(arg2, 'r')
        resultAgents = {}
        notFullyInjected = []
        for request in listOpen:
            request = request.strip()
            result = checkWorkQueue(request)
            for activeAgent in result['ActiveAgents']:
                if activeAgent not in resultAgents:
                    resultAgents[activeAgent] = []
                resultAgents[activeAgent].append(request)
            if result['ElementsAvailable'] or result['ElementsAcquired']:
                notFullyInjected.append(request)
        print "WorkQueue Analysis Results:"
        print "---------------------------"
        for agent in resultAgents:
            print "Agent %s:" % agent
            for request in sorted(resultAgents[agent]):
                print request
        print "---------------------------"
        print "Requests not fully injected yet"
        for request in notFullyInjected:
            print request

if __name__ == '__main__':
    sys.exit(main())