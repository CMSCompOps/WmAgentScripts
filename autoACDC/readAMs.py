import os, sys
import argparse
import json
from collections import defaultdict

sys.path.append('..')

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
	workflow x task x errorCodes
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

def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n-1, type))


def main():

	# scipt parameters
	parser = argparse.ArgumentParser(description='Famous Submitter')
	parser.add_argument("-q"   , "--query", 	type=str, help="Query", required=True)
	parser.add_argument("-o"   , "--output" , 	type=str, default="wf_list.txt", help="Output file")
	parser.add_argument("-t"   , "--test" , 	action="store_true", help="Doesn't submit ACDCs")

	options = parser.parse_args()
	query = options.query
	outputFile = options.output

	# get wokrflow infos
	result = getAMsFromQuery(query)

	# get dictionary of errors: workflows x tasks x errors
	wfToFix = getDictOfErrors(result)

	# loop over workflows, tasks, for each create ACDC and assign it
	# using default or custom configurations
	for iWorkflow, (wf, tasks) in enumerate(wfToFix.items()):

		if iWorkflow > 2: break

		print('-->',wf)

		for task, errorCodes in tasks.items():

			print('\t|-->', task)

			# default configs
			memory = None
			xrootd = False
			exclude_sites = []
			splitting = ''		# Uses: '(number)x', 'Same', 'max'

			for err in errorCodes:

				# add custom configs
				if err == '8001' and query == 'exitCodeSite = 8001-T2_CH_CERN_HLT':
					exclude_sites += ['T2_CH_CERN_HLT', 'T2_CH_CERN']
					xrootd = True

				if err == '8001' and query == 'exitCodeSite = 8001-T3_US_NERSC':
					pass

				if err == '50664':
					splitting = '10x'

				if err == '99109' and query == "exitCodeSite = 99109-T2_IT_Bari":
					exclude_sites += ['T2_IT_Bari']

				# other custom configs ...

			
			if options.test: 
				continue

			# make ACDC
			auto = autoACDC(task, testbed=False, testbed_assign=False,
							splitting=splitting,
							memory=memory, xrootd=xrootd, exclude_sites=exclude_sites)
			try:
				auto.go()
				with open(outputFile, 'a') as f: f.write(task+', '+auto.acdcName+'\n') 
			except Exception as e:
				print("Failed submission with excpetion", e)
				with open(outputFile, 'a') as f: f.write(task+', '+auto.acdcName+', '+str(e)+'\n') 
			
			print("#####################################################################\n\n")


if __name__ == "__main__":
    main()
