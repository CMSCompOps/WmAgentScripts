import os
import argparse
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
parser = argparse.ArgumentParser(description='Famous Submitter')
parser.add_argument("-q"   , "--query" , type=str, help="Query", required=True)
parser.add_argument("-o"   , "--output" , type=str, default="wf_list.txt", help="Output file")

options = parser.parse_args()
query = options.query
outputFile = options.output

# get workflows that match query
result = getAMsFromQuery(query)
workflows = getAllWorkflows(result)

# grab only NANO workflows
print("WARNING: Only grabbing NANO workflows.")
workflows = [w for w in workflows if 'nano' in w.lower()]

# and write them to file
with open(outputFile, 'w') as outfile:
  outfile.write('\n'.join(str(i) for i in workflows))
