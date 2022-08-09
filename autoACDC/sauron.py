""""
SAURON:
Semi AUtomatic Resubmission ONline

Submits ACDCs with tasks that match a query en masse.
e.g.
python sauron.py --query "exitCode = 50664" 
				--customise '{"8001": {"exclude_sites": ["T2_CH_CERN", "T2_CH_CERN_HLT"], 
										"xrootd": "enabled"}, 
							 "50664": {"splitting": "10x"}}'

Author: Luca Lavezzo
Date: July 2022
"""

import os, sys
import argparse
import json
from collections import defaultdict

sys.path.append('..')
sys.path.append('../Unified')

from utils import workflowInfo
from autoACDC import autoACDC

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

def getDictOfErrors(result: dict):
	"""
	compile information in a nested dictionary with dimensions
	workflow x task x exitCodes
	"""
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

	return wfToFix

def updateConfigs(configs, solutions):
	for skey, sitem in solutions.items():

		# check that the proposed parameter to change exists
		if skey in configs.keys():

			# we need to append to this list, not overwrite everytime
			if skey == 'exclude_sites' or skey == 'include_sites':
				if type(skey) == list:
					configs[skey] += sitem
				elif type(skey) == str:
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
	with open(file, 'r') as f: lines = f.readlines()
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
	parser.add_argument("-q"	, "--query",	type=str, help="Query to pass to search tool", required=True)
	parser.add_argument("-c"	, "--customise",type=str, help="Dictionary of exitCodes and solutions.", required=True)
	parser.add_argument("-o"	, "--output" , 	type=str, default="wf_list.txt", help="Output file")
	parser.add_argument("-t"	, "--test" , 	action="store_true", help="Doesn't submit ACDCs")

	options = parser.parse_args()
	query = options.query
	outputFile = options.output
	solutions_dict = json.loads(options.customise)

	# get wokrflow infos
	result = getAMsFromQuery(query)

	# get dictionary of errors: workflows x tasks x errors
	wfToFix = getDictOfErrors(result)

	print("Found", len(wfToFix.keys()), "workflows matching this query.")
	
	# loop over workflows, tasks, for each create ACDC and assign it
	# using default or custom configurations
	for iWorkflow, (wf, tasks) in enumerate(wfToFix.items()):

		print('-->',wf)

		for task, errorCodes in tasks.items():

			print('\t|-->', task)

			# default configs
			configs = {
				"memory" : None,
				"xrootd" : False,
				"include_sites" : [],
				"exclude_sites" : [],
				"splitting" : ''		# Uses: '(number)x', 'Same', 'max'
			}
			
			# for each error code in the task, if the errorCode is specified
			# in the customisation, we update the configurations with the
			# proposed solutions
			for err in errorCodes:
				for key, solutions in solutions_dict.items():
					if err == key:
						configs = updateConfigs(configs, solutions)

			
			if options.test:
				print(configs)
				continue

			# make ACDC
			auto = autoACDC(task, testbed=False, testbed_assign=False,
							splitting=splitting, memory=memory, xrootd=xrootd,
							include_sites=include_sites, exclude_sites=exclude_sites)

			try:
				auto.go()
				with open(outputFile, 'a') as f: f.write(task+', '+auto.acdcName+'\n') 
			except Exception as e:
				print("Failed submission with excpetion", e)
				with open(outputFile, 'a') as f: f.write(task+', '+auto.acdcName+', '+str(e)+'\n') 
			
			print("#####################################################################\n\n")


if __name__ == "__main__":
    main()
