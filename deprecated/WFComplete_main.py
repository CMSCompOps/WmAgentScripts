#!/usr/bin/env python

import json, os, httplib, sys
from deprecated import dbsTest
import datetime
import subprocess
from string import Template,find
from optparse import OptionParser, OptionGroup
from copy import deepcopy as dupli

red='\033[0;31m'
gre='\033[0;32m'
yel='\033[0;33m'
blu='\033[0;34m'
pur='\033[0;35m'
cya='\033[0;36m'
dfa='\033[m'

here=os.getcwd()

log_cmst1="/afs/cern.ch/user/c/cmst1/scratch0/WFFinalize/WFComplete.log"
log_cmst1_out="/afs/cern.ch/user/c/cmst1/scratch0/WFFinalize/WFComplete_output.log"

def createOptionParser():
	parser = OptionParser()
	group1 = OptionGroup(parser,"Essential options","Nothing will happen if you don't provide one or more of these options.")
	group2 = OptionGroup(parser,"Additional options","Pick at will.")
	group1.add_option("-d","--dbstest",help="get progress from dbs",action="store_true",default=False)
	group1.add_option("-c","--complete",help="complete workflows",action="store_true",default=False)
	group1.add_option("-s","--status",help="wf status from reqmon",action="store_true",default=False)
	group1.add_option("-a","--agent",help="wf agent from reqmon",action="store_true",default=False)
	group2.add_option("-v","--verbose",help="maximize output",action="store_true",default=False)
	group2.add_option("-n","--norun",help="don't run the created complete scripts",action="store_true",default=False)
	group2.add_option("-t","--testconnection",help="don't run anything but some dummy commands to test connections",action="store_true",default=False)
	group2.add_option("-f","--file",dest="filename",help="load workflow names from file")
	parser.add_option_group(group1)
	parser.add_option_group(group2)
	return parser

def appendFile(filename,lines):
	fileOut = file(filename,'a+')
	fileOut.write(lines)
	fileOut.close()

def getOverviewRequests(options):
	url = 'vocms204.cern.ch'
	conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1 = conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
	print cya+"Overview request: connection made, getting response..."+dfa
	r2 = conn.getresponse()
	try:
		requests = json.read(r2.read())
	except:
		print red+"Request overview didn't return valid file, please try again. Exiting."+dfa
		sys.exit()
	requests_dict = {}
	print cya+"Overview request done."+dfa
	for r in requests:
		requests_dict[r['request_name']] = r
	if options.verbose:
		 print cya+"Overview request output dictionarized."+dfa
	print
	return requests_dict

def readWorkflowList(options): 
	wfs_dict = {}
	wfs_dict_skipped = {}
	done = False
	appendFile(log_cmst1,"== "+str(datetime.datetime.now())+" == workflows input ==\n")
	logLines = ""
	if options.filename:
		print cya+"Reading workflow names from file: "+options.filename+dfa
		f = file(options.filename,'r+')
		for wf_raw in f:
			wf = str(wf_raw.strip())
			if wf == "":
				continue
			if wf in wfs_dict.keys():
				print "%s%s was given twice as input, only saved it once.%s" % (red,wf,dfa)
                                continue
			wfs_dict[wf] = {'request_name':wf}
			logLines += '\t'+wf+'\n'
		f.close()
	else:
		print cya+'List of workflow names? '+dfa
		while not done:
			wf = str(raw_input('').strip())
		        if wf == "":
	        	        done = True
	                	continue
			if wf in wfs_dict.keys():
				print "%s%s was given twice as input, only saved it once.%s" % (red,wf,dfa)
				continue
		        wfs_dict[wf] = {'request_name':wf}
			logLines += '\t'+wf+'\n'
	if options.verbose:
		print cya+"Workflow dictionary created."+dfa
	appendFile(log_cmst1,logLines)
	return wfs_dict,wfs_dict_skipped

def getDbsProgress(options,wfs_dict,wfs_dict_skipped):
	print cya+"Getting progress from dbs..."+dfa
	url = "cmsweb.cern.ch"
	for wf in wfs_dict.keys():
		wfs_dict[wf]['dbsProgress'] = []
		try:
			outputDataSets = deprecated.dbsTest.phedexSubscription.outputdatasetsWorkflow(url, wf)
		except:
			print "\t%s'%s' cannot be looked up in dbs, skipping.%s" % (red,wf,dfa)
			wfs_dict_skipped[wf] = wfs_dict[wf]
                        del wfs_dict[wf]
			continue
		try:
			inputEvents = deprecated.dbsTest.getInputEvents(url, wf)
		except:
			print "\t%s'%s' cannot be looked up in dbs, skipping.%s" % (red,wf,dfa)
			wfs_dict_skipped[wf] = wfs_dict[wf]
			del wfs_dict[wf]
			continue
		for dataset in outputDataSets:
			outputEvents = deprecated.dbsTest.getEventCountDataSet(dataset)
			wfs_dict[wf]['dbsProgress'].append({"dataset":dataset,"progress":str(outputEvents/float(inputEvents)*100)})
	if options.verbose:
		print cya+"Added dbs progress info to workflow dictionary."+dfa
        appendFile(log_cmst1,"== "+str(datetime.datetime.now())+" == progress queried from dbs ==\n")
	return wfs_dict,wfs_dict_skipped

def matchOverviewRequests(options,wfs_dict,requests_dict,wfs_dict_skipped):
	wfs_dict_noneed = {}
	wfs_dict_problem = {}
	print cya+"Matching workflows to reqmon data..."+dfa
	for wf in wfs_dict.keys(): #campaign, site_whitelist, global_queue, request_id, type, total_jobs
		try:
			request = requests_dict[wf]
		except:
			print "%s'%s' is not in overview requests list, skipping.%s" % (red,wf,dfa)
			wfs_dict_skipped[wf] = wfs_dict[wf]
			del wfs_dict[wf]
			continue
		try:
			wfs_dict[wf]['status'] = requests_dict[wf]['status']
		except:
                        print "%s'%s' is invalid in overview requests list (no status), skipping.%s" % (red,wf,dfa)
			wfs_dict_skipped[wf] = wfs_dict[wf]
                        del wfs_dict[wf]
                        continue
		if not ( wfs_dict[wf]['status'] == 'announced' or wfs_dict[wf]['status'] == 'completed' or wfs_dict[wf]['status'] == 'closed-out'):
			try:
        	                wfs_dict[wf]['local_queue'] = requests_dict[wf]['local_queue']
			except:
	                        wfs_dict_problem[wf] = wfs_dict[wf]
        	                del wfs_dict[wf]
                	        continue
		else:
                        wfs_dict_noneed[wf] = wfs_dict[wf]
                        del wfs_dict[wf]
                        continue
	if options.verbose:
		print cya+"Inserted reqmon data in workflow dictionary."+dfa
	print
	appendFile(log_cmst1,"== "+str(datetime.datetime.now())+" == reqmon data retrieved ==\n")
	return wfs_dict,wfs_dict_skipped,wfs_dict_noneed,wfs_dict_problem

def completeWorkflows(options,wfs_dict,wfs_list,wfs_dict_skipped):
	print cya+"Completing workflows on each agent..."+dfa
	manage_complete="./config/wmagent/manage execute-agent wmagent-workqueue -i"
	cmd_complete=Template("workqueue.doneWork(WorkflowName = '$wf_')")
	wfs_dict_completed = {}
	save_queue = ""
	queues = []
	for wf in sorted(wfs_list, key=lambda k:k['local_queue']):
		## skip multi_queue cases
		if len(wf['local_queue']) > 1:
			print red+"Workflow running on more than one queue, not executing automated completion."+dfa
			wfs_dict_skipped[wf] = wf
			del wfs_dict[wf['request_name']]
			wfs_list.remove(wf['request_name'])
			continue
		## actions separated by local_queue: write log and script
		this_queue = wf['local_queue'][0]
		this_qname = this_queue[7:find(this_queue,'.')]
		if not save_queue == this_queue:
			save_qname = save_queue[7:find(save_queue,'.')]
			script1FileSh = here+"/submit_"+save_qname+".sh"
			script2FileSh = here+"/complete_"+save_qname+".sh"
			
			if not save_queue == "":
				appendFile(log_cmst1,logLines+"\toutput in "+log_cmst1_out+"\n")
				if os.path.exists(script2FileSh):
                                        os.remove(script2FileSh)
				appendFile(script2FileSh,script2Lines)
				if os.path.exists(script1FileSh):
                                        os.remove(script1FileSh)
				appendFile(script1FileSh,script1Lines)
			else:
				appendFile(log_cmst1,"== "+str(datetime.datetime.now())+" == writing completing scripts for ==\n")
			# save queuename
			save_queue = this_queue
                        queues.append(this_queue)
			# reset log & scripts
			logLines = ""
			submitFileSh = here+"/WFComplete_submitter.sh"
			script1FileSh = here+"/submit_"+this_qname+".sh"
			script2FileSh = here+"/complete_"+this_qname+".sh"
                        script1FileShTmp = "/tmp/submit_"+this_qname+".sh"
                        script2FileShTmp = "/tmp/complete_"+this_qname+".sh"
			path=this_qname+".cern.ch"
                        print "Workflows to complete for %s%s%s %s" % (gre,this_queue,dfa,script2FileSh)
			script1Lines = "#!/bin/sh\n\n\
scp "+script2FileSh+" "+path+":"+script2FileShTmp+"\n\
ssh -t "+path+" "+submitFileSh+" "+script2FileShTmp+" >> "+log_cmst1_out+"\n\
rm "+script2FileSh+"\n"
                	script2Lines = "#!/bin/sh\n\nsource ~cmst1/.bashrc\nagentenv\n"
		## add each workflow
		wf_cmd_complete = cmd_complete.safe_substitute(wf_=wf['request_name'])
		logLines += "\t%-75s%-20s\n" % (wf['request_name'],this_qname)
		if not options.testconnection: # run actual complete commands
			script2Lines += "echo \""+wf_cmd_complete+"; quit()\" | "+manage_complete+"\n"
		else: # run test commands
			script2Lines += "echo \"print 'Hello, your connection works.'; quit()\" | "+manage_complete+"\n"
		if options.verbose:
			print "%-150s" % wf_cmd_complete
	## write log and script for last local_queue in the list
	appendFile(log_cmst1,logLines+"\toutput in "+log_cmst1_out+"\n")
	script2FileSh = here+"/complete_"+this_qname+".sh"
        script1FileSh = here+"/submit_"+this_qname+".sh"
	if os.path.exists(script2FileSh):
        	os.remove(script2FileSh)
	appendFile(script2FileSh,script2Lines+"exit\n")
        if os.path.exists(script1FileSh):
                os.remove(script1FileSh)
        appendFile(script1FileSh,script1Lines+"exit\n")


	## complete workflows and write logs, skip if norun flag set
	if not options.norun:
		for q in queues:
			qname=q[7:find(q,'.')]
                        print "Completing on %s" % qname
			submitFileSh = here+"/WFComplete_submitter.sh"
			script1FileSh = here+"/submit_"+qname+".sh"
			script2FileSh = here+"/complete_"+qname+".sh"
			appendFile(log_cmst1_out,"== "+str(datetime.datetime.now())+" == workqueue.doneWork() results for "+q+" ==\n")
			proc = subprocess.Popen(['sh',script1FileSh,'>>',log_cmst1_out],shell=False,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
			appendFile(log_cmst1_out,proc.stdout.read())
			proc.wait()
		print "%sWorkflow completion done. See log %s %s\n" % (cya,dfa,log_cmst1)

	return wfs_dict,wfs_list,wfs_dict_skipped

#===============================================================================

def main():
	parser = createOptionParser()
	(options,args) = parser.parse_args()

	if not options.dbstest and not options.agent and not options.status and not options.complete:
		print red+"No work to do, exiting. Check syntax: "+dfa
		parser.print_help()
		sys.exit()
	
	# get workflow names
	wfs_dict,wfs_dict_skipped = readWorkflowList(options)

	print cya+str(len(wfs_dict.keys())+len(wfs_dict_skipped.keys()))+" workflow names entered."+dfa
	print
	
	# [optional] get and print progress results from dbs
	if options.dbstest: 
		wfs_dict,wfs_dict_skipped = getDbsProgress(options,wfs_dict,wfs_dict_skipped)
		for wf in wfs_dict.keys():
			for dataset in wfs_dict[wf]['dbsProgress']:
        	        	print "\t%-75s %-110s %s%20s%s%%" % (wf,dataset['dataset'],gre,dataset['progress'],dfa)
		print

	# get requests overview if needed and match workflows to requests
	if options.agent or options.status or options.complete:
		requests_dict = getOverviewRequests(options)
		wfs_dict,wfs_dict_skipped,wfs_dict_noneed,wfs_dict_problem = matchOverviewRequests(options,wfs_dict,requests_dict,wfs_dict_skipped)

		# make dict (faster) available as list (sortable)
		wfs_list = []
		for wf in wfs_dict.keys():
			wfs_list.append(wfs_dict[wf])
		wfs_list_noneed = []
		for wf in wfs_dict_noneed.keys():
			wfs_list_noneed.append(wfs_dict_noneed[wf])
	        wfs_list_problem = []
		for wf in wfs_dict_problem.keys():
			wfs_list_problem.append(wfs_dict_problem[wf])

	# [optional] print agents
	if options.agent and not options.status:
		print cya+"Workflows per agent:"+dfa
		for wf in sorted(wfs_list, key=lambda k: k['local_queue']):
			print "%-75s %-50s" % (wf['request_name'], wf['local_queue'])
		print

	# [optional] print statuses
	elif options.status and not options.agent:
		print cya+"Workflows per status:"+dfa
		for wf in sorted(wfs_list, key=lambda k:k['status']):
			print "%-75s %-50s" % (wf['request_name'], wf['status'])
		print
                for wf in sorted(wfs_list_noneed, key=lambda k:k['status']):
                        print "%-75s %-50s" % (wf['request_name'], wf['status'])
                print
		if len(wfs_list_problem) > 0:
			print cya+"Workflows per status (running but no local_queue):"+dfa
        	        for wf in sorted(wfs_list_problem, key=lambda k:k['status']):
	               	        print "%-75s %-50s" % (wf['request_name'], wf['status'])
        	        print

	# [optional] print agents & statuses
	elif options.status and options.agent:
		print cya+"Workflows per status:"+dfa
                for wf in sorted(wfs_list, key=lambda k:(k['status'],k['local_queue'])):
                        print "%-75s %-50s %-50s" % (wf['request_name'], wf['status'], wf['local_queue'])
                print
		for wf in sorted(wfs_list_noneed, key=lambda k:k['status']):
                        print "%-75s %-50s" % (wf['request_name'], wf['status'])
                print
		if len(wfs_list_problem) > 0:
	                print cya+"Workflows per status (running but no local_queue):"+dfa
	                for wf in sorted(wfs_list_problem, key=lambda k:k['status']):
        	                print "%-75s %-50s" % (wf['request_name'], wf['status'])
	                print
	
	# complete workflows
	if options.complete:
		if not options.agent:
			if len(wfs_list_noneed) > 0:
		                print cya+"Workflows per status (already completed/announced):"+dfa
        		        for wf in sorted(wfs_list_noneed, key=lambda k:k['status']):
                		        print "%-75s %-50s %-50s" % (wf['request_name'], wf['status'], wf['local_queue'])
		                print
			if len(wfs_list_problem) > 0:
				print cya+"Workflows per status (running but no local_queue):"+dfa
				for wf in sorted(wfs_list_problem, key=lambda k:k['status']):
	                                print "%-75s %-50s" % (wf['request_name'], wf['status'])
	                        print
		wfs_dict,wfs_list,wfs_dict_skipped = completeWorkflows(options,wfs_dict,wfs_list,wfs_dict_skipped)
	if options.norun:
		print red+"! Scripts were created, but not run at this time."+dfa

	wfs_list = []
	for wf in wfs_dict.keys():
                wfs_list.append(wfs_dict[wf])
	wfs_list_skipped = []
	for wf in wfs_dict_skipped.keys():
		wfs_list_skipped.append(wfs_dict_skipped[wf])

	if (options.status or options.agent) and len(wfs_list_skipped) > 0:
		print red+"Skipped workflows:"+dfa
		for wf in sorted(wfs_list_skipped, key=lambda k:k['status']):
			if "local_queue" in wf.keys() and "status" in wf.keys():
	                        print "%-75s %-50s %-50s" % (wf['request_name'], wf['status'], wf['local_queue'])
			elif "local_queue" in wf.keys():
				print "%-75s %-50s" % (wf['request_name'], wf['local_queue'])
			elif "status" in wf.keys():
				print "%-75s %-50s" % (wf['request_name'], wf['status'])
                print
	
	if options.agent or options.status or options.complete:
		print cya+str(len(wfs_list)+len(wfs_list_skipped)+len(wfs_list_noneed)+len(wfs_list_problem))+" workflow names parsed."+dfa
	else: 
		print cya+str(len(wfs_list)+len(wfs_list_skipped))+" workflow names parsed."+dfa
	print

if __name__ == "__main__":
	main()

	

	

