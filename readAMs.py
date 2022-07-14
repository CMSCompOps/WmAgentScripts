import os
import argparse
import json
from collections import defaultdict

def getAMsFromQuery(query: str):
	""" Get assistance-manuals from a query """
	string = """curl -X POST -H "Content-Type: application/json" -d '{{"query": "{query}"}}' tni-test.cern.ch/search/search""".format(query=query)
	result = json.loads(os.popen(string).read())
	return result

def getAllWorkflows(wfDict: dict):
	""" Get all workflows from dictionary """
	return list(wfDict.keys())

def getTasksAffectedByError(wfDict: dict, exitCode: str):
	""" Get all tasks in a workflow that have an exitCode """
	wfErrorsDict = wfDict['errors']
	affectedTasks = []
	for task, taskDict in wfErrorsDict.items():
		exitCodes = list(taskDict.keys())
		if exitCode in exitCodes: affectedTasks.append(task)
	return affectedTasks

def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n-1, type))

# scipt parameters
parser = argparse.ArgumentParser(description='Famous Submitter')
parser.add_argument("-q"   , "--query" , type=str, help="Query", required=True)
parser.add_argument("-o"   , "--output" , type=str, default="wf_list.txt", help="Output file")

options = parser.parse_args()
query = options.query
outputFile = options.output

# get workflows that match query
result = getAMsFromQuery(query)
workflows = getAllWorkflows(result)

# and write them to file
with open(outputFile, 'w') as outfile:
  outfile.write('\n'.join(str(i) for i in workflows))

# compile information in a nested dictionary with form
# workflow x task x errorCodes
wfToFix = nested_dict(2, str)
for wf in result.keys():

	# all tasks that failed in this wf
	errors = result[wf]['errors']
	failedTasks = list(errors.keys())

	# only relevant tasks that failed in this wf
	for task in failedTasks:
		if any([sub in task for sub in ['LogCollect', 'Cleanup']]):
			errors.pop(task)

	# save dictionary
	failedTasks = list(errors.keys())
	for task in failedTasks:
		wfToFix[wf][task] = list(errors[task].keys())

# loop over workflows, tasks, for each create ACDC and assign it
# using default or custom configurations
for wf, tasks in wfToFix.items():

	for task, errorCodes in tasks.items():

		# default configs
		mem = None
		xrd = False
		exclude = ''

		# file written by makeACDC.py to save the name of the new workflow-turned task
		# read in by assign.py to know what it has to assign
		out = 'acdc.txt'

		for err in errorCodes:

			# add custom configs
			if err == '8001':
				exclude = 'T2_CH_CERN_HLT, T2_CH_CERN'

			# other custom configs ...

		# make ACDC
		os.system('makeACDC.py --path {} --memory {} --xrootd {} --mcore {} --out {}'.format(task, mem, xrd, mcore, out))

		# assign it
		os.system('assign.py --memory {} --file {} --exclude {} --checksite --sites acdc'.format(mem, out, exclude))