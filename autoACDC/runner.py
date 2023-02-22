""""
runner.py

Submits ACDCs with tasks that match a query en masse.
e.g.
python runner.py
    --query "exitCode = 50664"
    --customise '{
                "8001": {"xrootd": "enabled"},
                "T2_US_MIT": {"splitting": "10x"},
                "8001-T2_CH_CERN_HLT": {
                  "exclude_sites": ["T2_CH_CERN", "T2_CH_CERN_HLT"],
                  "xrootd": "enabled"
                },
                "RunIISummer": {"xrootd": 1}}'

Author: Luca Lavezzo
Date: July 2022
"""

import os
import sys
import argparse
import json
from collections import defaultdict

sys.path.append('..')
sys.path.append('../Unified')

from rejectWorkflows import rejectWorkflow
from utils import workflowInfo
from autoACDC import autoACDC


def getQueryFromJSON(infile):
    query = ''
    if not os.path.isfile(infile):
        raise FileNotFoundError
    with open(infile, 'r') as f:
        query_list = json.load(f)['query']
        for i, expr in enumerate(query_list):
            # double bagging
            expr = "(" + expr + ")"
            # append
            if i == 0:
                query = expr
            else:
                query = "(" + expr + " OR " + query + ")"
    return query


def getAMsFromQuery(query: str):
    """ Get assistance-manuals from a query """
    string = """curl -X POST -H "Content-Type: application/json" -d '{{"query": "{query}"}}' tni-test.cern.ch/search/search""".format(query=query)
    result = json.loads(os.popen(string).read())
    return result


def getAllWorkflows(wfDict: dict):
    """ Get all workflows from dictionary """
    return list(wfDict.keys())


def getTasksWithError(wfDict: dict, exitCode: str):
    """ Get all tasks in a workflow that have an exitCode """
    wfErrorsDict = wfDict['errors']
    affectedTasks = []
    for task, taskDict in wfErrorsDict.items():
        exitCodes = list(taskDict.keys())
        if exitCode in exitCodes:
            affectedTasks.append(task)
    return affectedTasks


def applySolutions(configs, task_dict, solutions_dict):
    """
    Looks through the task information and matches it
    with any of the proposed solution, updating the
    configurations.

    Returns: dict of configurations
    """

    if 'errors' not in list(task_dict.keys()):
        sys.exit()

    for attr, solution in solutions_dict.items():

        # exitCode
        if attr.isnumeric():
            exitCode = attr
            if exitCode in list(task_dict['errors'].keys()):
                configs = updateConfigs(configs, solution)

        # exitCodeSite
        elif '-' in attr and attr.split('-')[0].isnumeric() and any(site in attr.split('-')[1] for site in ['T0','T1','T2','T3']):
            exitCode, site = attr.split("-")
            if exitCode in list(task_dict['errors'].keys()):
                if site in list(task_dict["errors"][exitCode].keys()):
                    configs = updateConfigs(configs, solution)

        # site
        elif any(site in attr for site in ['T0', 'T1', 'T2', 'T3']):
            site = attr
            for exitCode in list(task_dict['errors'].keys()):
                if site in list(task_dict['errors'][exitCode].keys()):
                    configs = updateConfigs(configs, solution)

        # campaign
        else:
            campaign = attr
            if campaign in list(task_dict['campaigns']):
                configs = updateConfigs(configs, solution)

    return configs   


def checkType(key, item):
    if key == 'memory':
        return type(item) is str
    elif key == 'xrootd' or key == 'secondary_xrootd':
        if item.lower() == 'true' or item.lower() == 'false':
            return True
        else:
            return type(item) is bool
    elif key == 'include_sites':
        return (type(item) is str or type(item) is list)
    elif key == 'exclude_sites':
        return (type(item) is str or type(item) is list)
    elif key == 'splitting':
        return type(item) is str
    elif key == 'exceptions':
        return type(item) is dict
    else:
        raise Exception(key + " not accepted.")


def updateConfigs(configs, solutions):
    """
    Update configs dictionary. Checks type, and makes sure you are not adding
    any keys that do not already exist, or overwriting things that don't need
    to be overwritten.
    """
    for skey, sitem in solutions.items():

        # check that the proposed parameter to change exists
        if skey in configs.keys():

            # check type
            if not checkType(skey, sitem):
                raise Exception(skey + " is wrong type.")

            # we need to append to this list, not overwrite everytime
            if skey == 'exclude_sites' or skey == 'include_sites':
                if type(sitem) == list:
                    configs[skey] += sitem
                elif type(sitem) == str:
                    configs[skey].append(sitem)
                else:
                    raise Exception(type(skey) + " not an accepted type for " + skey)

            # else, we can overwrite
            else:
                configs[skey] = sitem

        else:
            raise Exception(skey + " not in allowed configs.")

    return configs


def readInSubmittedTasks(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    tasks = [line.split(', ')[0] for line in lines]
    return tasks


def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n-1, type))


def main():

    # scipt parameters
    parser = argparse.ArgumentParser(description='Famous Submitter')
    parser.add_argument("-q", "--query", type=str, help="Query to pass to search tool, or path to .json file", required=True)
    parser.add_argument("-c", "--customise", type=str, help="Dictionary of exitCodes and solutions, or path to .json file containing dict.", required=True)
    parser.add_argument("-e", "--exceptions", type=str, help="Dictionary of exceptions, or path to .json file containing dict.", required=False, default=None)
    parser.add_argument("-o", "--output", type=str, default="wf_list.txt", help="Output file")
    parser.add_argument("-t", "--test", action="store_true", help="Doesn't submit ACDCs")

    options = parser.parse_args()
    query = options.query
    outputFile = options.output

    # load query
    if options.query.endswith('.json'):
        query = getQueryFromJSON(options.query)
    else:
        query = options.query

    # load solutions
    if '.json' in options.customise:
        with open(options.customise, 'r') as f:
            solutions_dict = json.load(f)
    else:
        solutions_dict = json.loads(options.customise)

    # load exceptions
    if options.exceptions is None:
        exceptions_dict = {}
    elif '.json' in options.exceptions:
        with open(options.exceptions, 'r') as f:
            exceptions_dict = json.load(f)
    else:
        exceptions_dict = json.loads(options.exceptions)

    # get wokrflow infos
    result = getAMsFromQuery(query)

    print("Found", len(result.keys()), "workflows matching this query.")

    # loop over workflows, tasks, for each create ACDC and assign it
    # using default or custom configurations
    for iWorkflow, (wfName, workflow) in enumerate(result.items()):

        print('-->', wfName)

        for task, attributes in workflow["tasks"].items():

            print('\t|-->', task)

            # based on the tasks' attributes, apply the solutions
            # default configs
            configs = {
                "memory": None,
                "xrootd": False,
                "secondary_xrootd": False,
                "include_sites": [],
                "exclude_sites": [],
                "splitting": '',        # Uses: '(number)x', 'Same', 'max'
                "exceptions": exceptions_dict
            }
            configs = applySolutions(configs, attributes, solutions_dict)

            if options.test:
                print(configs)
                continue

            # make ACDC
            auto = autoACDC(task, testbed=False, testbed_assign=False, **configs)

            try:
                auto.go()
                with open(outputFile, 'a') as f:
                    f.write(task+', '+auto.acdcName+'\n')
            except Exception as e:
                if auto.acdcName:
                    rejectWorkflow(auto.acdcName, url='cmsweb.cern.ch', invalidate=True)
                print("Failed submission with exception", e)
                with open(outputFile, 'a') as f:
                    f.write(task+', '+auto.acdcName+', '+str(e)+'\n')

            print("#####################################################################\n\n")


if __name__ == "__main__":
    main()
