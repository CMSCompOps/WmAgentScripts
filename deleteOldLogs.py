"""
Delete logs in PRODUCTION for workflows in *-archived that have been archived
more than maxAgeMonths months ago.

Author: Luca Lavezzo
Date: February 2023
"""


import os
import time
import logging
import shutil

from utils import workflowInfo, siteInfo


def formLogList(fname):
	try:
		os.system('eos ls /eos/cms/store/logs/prod/recent/PRODUCTION > {}'.format(fname))
	except Exception as e:
		logging.error("Quitting, failed with Exception: {}".format(e))
		sys.exit()

def readLogList(fname):
	with open(fname, 'r') as f:
		lines = f.readlines()
	return lines

def deleteLogList(fname):
	os.remove(fname)

def getAgeInMonths(when):
	currentTime = time.time()
	timeDiffDays = (currentTime - when) // 86400
	timeDiffMonths = timeDiffDays // 30
	return timeDiffMonths

def checkArchivedWhen(schema):
	requestTransition = schema['RequestTransition']
	isArchived = False
	whenArchived = -1
	for step in requestTransition:
		status = step.get('Status')
		if type(status) is not str: continue

		if status.endswith('archived'):
			if not isArchived:
				isArchived = True
				whenArchived = step.get("UpdateTime")
			elif isArchived:
				if step.get("UpdateTime") > whenArchived:
					whenArchived = step.get("UpdateTime")

	return isArchived, whenArchived

def deleteLogs(logDir, wf):
	wfDir = logDir+'/'+wf
	try:
		shutil.rmtree(wfDir)
		return 1
	except Exception as e:
		logging.error("Failed to delete {} with Exception {}".format(wfDir, e))
		return 0

def main():

	# script parameters
	logDir = '/eos/cms/store/logs/prod/recent/PRODUCTION/'
	url = 'cmsweb.cern.ch'
	maxAgeMonths = 6
	
	logging.info("====================== INIT ======================")

	# define metadata to keep track of info
	metadata = {
		'deleted': 0,
		'failed': 0
	}

	# define output file name of all workflows in PRODUCTION
	start = time.time()
	today = time.strftime("%Y-%m-%d")
	fname = 'PRODUCTION-{}.txt'.format(today)

	# create and read list of files
	formLogList(fname)
	wfs = readLogList(fname)

	# delete old workflows
	for i, wf in enumerate(wfs):
		if i > 0: break				# testing
		
		# remove \n and such
		wf = wf.strip()

		# get workflow info
		wfInfo = workflowInfo(url, wf)
		schema = wfInfo.request

		# figure out if we have archived this, and when
		isArchived, whenArchived = checkArchivedWhen(schema)

		# if not archived, we don't want to delete
		if not isArchived: continue

		# check how many months ago it was archived
		monthsAge = getAgeInMonths(whenArchived)

		# if not older than maxAgeMonths months, keep it
		if monthsAge <= maxAgeMonths: continue

		# if older than maxAgeMonths months, delete its logs
		status = deleteLogs(logDir, wf)
		if status: metadata['deleted'] += 1
		else: metadata['failed'] += 1


	# delete list of files, since they get quite large ~10MB
	deleteLogList(fname)

	# calculate time it took the script to run
	end = time.time()
	metadata['minutes'] = (end-start)// 60 % 60

	# print metadata
	logging.info("Metadata:")
	logging.info(metadata)
	logging.info("====================== END ======================")
		

if __name__ == "__main__":
    main()