import os
from urllib.request import urlopen
import json

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


# scipt parameters
query = 'exitCode = 50660'
outputFile = 'wf_test.txt'

# get workflows that match query
result = getAMsFromQuery(query)
workflows = getAllWorkflows(result)

# and write them to file
with open(outputFile, 'w') as outfile:
  outfile.write('\n'.join(str(i) for i in workflows))