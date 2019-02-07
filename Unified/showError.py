#!/usr/bin/env python
from utils import workflowInfo, siteInfo, monitor_dir, monitor_pub_dir, base_dir, global_SI, getDatasetPresence, getDatasetBlocksFraction, getDatasetBlocks, unifiedConfiguration, getDatasetEventsPerLumi, dataCache, unified_url, base_eos_dir, monitor_eos_dir, unified_url_eos, eosFile, ThreadHandler, moduleLock
import time

import json
import sys
import os
from collections import defaultdict
from assignSession import *
import time
import optparse
import random
import threading

class ParseBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.deamon = True
        self.url = args.get('url')
        self.wfn = args.get('wfn')
        self.options = args.get('options')
        self.task_erro = None
        self.one_explanation = None
        
    def run(self):
        self.task_error, self.one_explanation = parse_one( self.url, self.wfn, self.options)
        

class AgentBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.deamon = True
        for k,v in args.items():
            setattr(self,k,v)

    def run(self):
        ## print are all mangled and not even worth adding at this point ...
        os.system('ssh %s %s/WmAgentScripts/Unified/exec_expose.sh %s %s %s %s %s %s'%( 
                self.agent, 
                self.base_eos_dir, 
                self.workflow, 
                self.wmbs,
                self.errorcode_s,
                self.base_eos_dir,
                self.monitor_eos_dir,
                self.task_short))
class ReadBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.daemon = True
        for k,v in args.items():
            setattr(self,k,v)

    def run(self):
        self.readable = os.system('XRD_REQUESTTIMEOUT=10 xrdfs root://cms-xrd-global.cern.ch stat %s'%self.file)
        

class XRDBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.daemon = True
        for k,v in args.items():
            setattr(self,k,v)

    def run(self):
        ## print are all mangled and not even worth adding at this point ...
        if True:
            if True:
                if True:
                    if True:
                        if True:
                            if True:
                                if True:
                                    
                                    os.system('mkdir -p /tmp/%s'%(os.getenv('USER')))
                                    local = '/tmp/%s/%s'%(os.getenv('USER'),self.out_lfn.split('/')[-1])
                                    #command = 'xrdcp root://cms-xrd-global.cern.ch/%s %s'%( self.out_lfn, local)
                                    command = 'XRD_REQUESTTIMEOUT=10 xrdcp root://cms-xrd-global.cern.ch/%s %s'%( self.out_lfn, local)
                                    print "#"*15,"cmsrun log retrieval","#"*15
                                    if os.system('ls %s'% local)!=0:
                                        print "running",command
                                        ## get the file
                                        xec = os.system( command )
                                        if xec !=0:
                                            print "\t\t",command,"did not succeed"
                                    else:
                                        print "the file",local,"already exists"
                                        xec = 0

                                    ## expose the content
                                    label=self.out_lfn.split('/')[-1].split('.')[0]
                                    m_dir = '%s/joblogs/%s/%s/%s/%s'%(self.monitor_eos_dir, 
                                                                      self.wfn, 
                                                                      self.errorcode_s,
                                                                      self.task_short,
                                                                      label)
                                    print "should try eos?"
                                    if os.system('ls %s'% local)!=0 and self.from_eos:
                                        print "no file retrieved using xrootd, using eos source"
                                        ## will parse eos and not doing anything for things already indexed
                                        os.system('Unified/createLogDB.py --workflow %s'%( self.wfn ))
                                        os.system('Unified/whatLog.py --workflow  %s --log %s --get' %(self.wfn,self.out_lfn.split('/')[-1]) )
                                        os.system('mv `find /tmp/%s/ -name "%s"` %s'%( os.getenv('USER'), self.out_lfn.split('/')[-1], local))

                                    if os.system('ls %s'% local)==0:
                                        os.system('mkdir -p %s'%(m_dir))
                                        os.system('tar zxvf %s -C %s'%(local,m_dir))
                                        ## truncate the content ??
                                        actual_logs = os.popen('find %s -type f'%(m_dir)).read().split('\n')
                                        for fn in actual_logs:
                                            if not fn: continue
                                            if not fn.endswith('log'): continue
                                            if any([p in fn for p in ['stdout.log']]):
                                                trunc = '/tmp/%s/%s'%(os.getenv('USER'), label)
                                                #print fn
                                                #print trunc
                                                head = tail = 1000
                                                os.system('(head -%d ; echo;echo;echo "<snip>";echo;echo ; tail -%d ) < %s > %s'%(head, tail, fn, trunc))
                                                os.system('mv %s %s.trunc.txt'%(trunc, fn))
                                    else:
                                        print "no file retrieved in",local




def parse_one(url, wfn, options=None):

    def time_point(label="",sub_lap=False):
        now = time.mktime(time.gmtime())
        nows = time.asctime(time.gmtime())

        print "[showError] Time check (%s) point at : %s"%(label, nows)
        print "[showError] Since start: %s [s]"% ( now - time_point.start)
        if sub_lap:
            print "[showError] Sub Lap : %s [s]"% ( now - time_point.sub_lap ) 
            time_point.sub_lap = now
        else:
            print "[showError] Lap : %s [s]"% ( now - time_point.lap ) 
            time_point.lap = now            
            time_point.sub_lap = now

    time_point.sub_lap = time_point.lap = time_point.start = time.mktime(time.gmtime())

    task_error_site_count ={}
    one_explanation = defaultdict(set)
    per_task_explanation = defaultdict(set)

    if wfn in ['vlimant_task_EXO-RunIISummer15wmLHEGS-04800__v1_T_170906_141738_1357']:
        return task_error_site_count, one_explanation

    time_point("Starting with %s"% wfn )
    threads = []

    SI = global_SI()
    UC = unifiedConfiguration()
    wfi = workflowInfo( url , wfn)
    time_point("wfi" ,sub_lap=True)
    where_to_run, missing_to_run,missing_to_run_at = wfi.getRecoveryInfo()       
    time_point("acdcinfo" ,sub_lap=True)
    all_blocks,needed_blocks_loc,files_in_blocks,files_and_loc_notin_dbs = wfi.getRecoveryBlocks()
    time_point("inputs" ,sub_lap=True)


    ancestor = workflowInfo( url , wfn)
    lhe,prim,_,sec = ancestor.getIO()
    high_order_acdc = 0
    while ancestor.request['RequestType'] == 'Resubmission':
        ancestor = workflowInfo(url, ancestor.request['OriginalRequestName'])
        lhe,prim,_,sec = ancestor.getIO()
        high_order_acdc += 1

    

    no_input = (not lhe) and len(prim)==0 and len(sec)==0

    cache = options.cache
    print "cache timeout", cache

    err= wfi.getWMErrors(cache=cache)
    time_point("wmerrors" ,sub_lap=True)
    stat = wfi.getWMStats(cache=cache)
    time_point("wmstats" ,sub_lap=True)
    #adcd = wfi.getRecoveryDoc()

    total_by_code_dash = defaultdict( int )
    total_by_site_dash = defaultdict( int )
    r_dashb =defaultdict( lambda : defaultdict( int ))
    dash_board_h = 1
    if False :
        ## NB get the since from when the wf has started, not a fixed value
        ## no dashboard until we get a better api
        #dashb = wfi.getFullPicture(since=dash_board_h,cache=cache)
        dashb = {}
        #print json.dumps( dashb , indent=2)
        for site,sinfo in dashb.items():
            for s_code,counts in sinfo.items():
                d_statuses = ['submitted','pending','app-unknown','done']
                total_by_code_dash[str(s_code)]+= counts.get('submitted',0)
                total_by_site_dash[site] += counts.get('submitted',0)
                r_dashb[str(s_code)][site] += counts.get('submitted',0)

        print json.dumps(total_by_code_dash , indent=2)
        print json.dumps(total_by_site_dash , indent=2)

    time_point("Got most input")

    status_per_task = defaultdict(lambda : defaultdict(int))
    
    if not 'AgentJobInfo' in stat:
        stat['AgentJobInfo'] = {}
        print "no information in AgentJobInfo, they agents must have been retired. I cannot go on without creating a partial report"
        return task_error_site_count, one_explanation 
        #print "bad countent ?"
        #print json.dumps(  stat,  indent=2)

    for agent in stat['AgentJobInfo']:
        for task in stat['AgentJobInfo'][agent]['tasks']:
            if not 'status' in stat['AgentJobInfo'][agent]['tasks'][task]: continue
            for status in stat['AgentJobInfo'][agent]['tasks'][task]['status']:
                info = stat['AgentJobInfo'][agent]['tasks'][task]['status'][status]
                #print status,stat['AgentJobInfo'][agent]['tasks'][task]['status'][status]
                if type(info)==dict:
                    status_per_task[task][status] += sum( stat['AgentJobInfo'][agent]['tasks'][task]['status'][status].values())
                else:
                    status_per_task[task][status] += stat['AgentJobInfo'][agent]['tasks'][task]['status'][status]

    #print json.dumps( status_per_task, indent=2)
    db_total_per_site = defaultdict(int) 
    db_total_per_code = defaultdict(int)
    ## cannot do that since there is no task count in dashboard and we have to take away the submitted
    #for site in dashb:
    #    for error in dashb[site]:
    #        db_total_per_site[site] += dashb[site][error] 
    #        db_total_per_code[code] += dashb[site][error]
    
    print "ACDC Information"
    print "\t where to re-run"
    print json.dumps( where_to_run , indent=2)         
    print "\t Missing events"
    print json.dumps(missing_to_run , indent=2)        
    print "\t Missing events per site"
    print json.dumps(missing_to_run_at , indent=2)        
    

    if not where_to_run and not missing_to_run and not missing_to_run_at:
        print "showError is unable to run"
        #return task_error_site_count, one_explanation
        pass

    do_JL = not options.no_JL
    do_CL = not options.no_CL
    do_all_error_code = options.all_errors
    if high_order_acdc>=1:
        print high_order_acdc,"order request, pulling down all logs"
        do_all_error_code = True
    if wfi.isRelval():
        print "getting all codes for relval"
        do_all_error_code = True
        

    tasks = sorted(set(err.keys() + missing_to_run.keys()))

    if not tasks:
        print "no task to look at"
        #return task_error_site_count

    html="<html> <center><h1>%s, Updated on %s (GMT)" % ( wfn, time.asctime(time.gmtime()) )    

    
    html+= '</center>'
    html += '<a href=https://cmsweb.cern.ch/reqmgr2/fetch?rid=%s>dts</a>, '%( wfn )
    html += '<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>ac</a>, '% ( wfi.request['PrepID'])
    html += '<a href=https://cms-gwmsmon.cern.ch/prodview/%s>Job Progress</a>, '%( wfn )
    r_type = wfi.request.get('OriginalRequestType', wfi.request.get('RequestType','NaT'))
    if r_type in ['ReReco']:
        html += '<a href=../datalumi/lumi.%s.html>Lumisection Summary</a>, '% wfi.request['PrepID']
    html += '<a href="https://its.cern.ch/jira/issues/?jql=text~%s AND project = CMSCOMPPR" target="_blank">jira</a>,'% (wfi.request['PrepID'])
    html += '<a href="https://vocms0113.cern.ch/seeworkflow/?workflow=%s">console</a>,'% wfn
    html += '<a href="http://vocms0276.cern.ch/tasks?page=1&filter=%s">new console</a>,'% wfn
    html += '<a href="http://wc-dev.cern.ch/tasks?page=1&filter=%s">dev console</a>'% wfn
    html+='<hr>'
    html += '<a href=#IO>I/O</a>, <a href=#ERROR>Errors</a>, <a href=#BLOCK>blocks</a>, <a href=#FILE>files</a>, <a href=#CODES>Error codes</a><br>'
    html+='<hr>'

    time_point("Header writen")

    html += '<a name=IO></a>'
    if prim:
        html+='Reads in primary<br>'
        rwl = wfi.getRunWhiteList()
        lwl = wfi.getLumiWhiteList()
        for dataset in prim:
            html +='<b>%s </b>(events/lumi ~%d)'%(dataset, getDatasetEventsPerLumi( dataset))
            blocks = getDatasetBlocks( dataset, runs= rwl ) if rwl else None
            blocks = getDatasetBlocks( dataset, lumis= lwl) if lwl else None
            available = getDatasetBlocksFraction(url, dataset, only_blocks = blocks )
            html +='<br><br>Available %.2f (>1 more than one copy, <1 not in full on disk)<br>'% available
            html +='<ul>'
            presence = getDatasetPresence(url, dataset, only_blocks = blocks )

            for site in sorted(presence.keys()):
                html += '<li>%s : %.2f %%'%( site, presence[site][1] )
            html+='</ul><br>'

            
    if sec:
        html+='Reads in secondary<br>'
        for dataset in sec:
            presence = getDatasetPresence(url, dataset)
            html +='<b>%s</b><ul>'%dataset
            for site in sorted(presence.keys()):
                html += '<li>%s : %.2f %%'%( site, presence[site][1] )
            html+='</ul>'
        

    outs = sorted(wfi.request['OutputDatasets'])
    if outs:
        html+='Produces<br>'
        for dataset in outs:
            presence = getDatasetPresence(url, dataset)
            html +='<b>%s </b>(events/lumi ~ %d)<ul>'%(dataset, getDatasetEventsPerLumi(dataset))
            for site in sorted(presence.keys()):
                html += '<li>%s : %.2f %%'%( site, presence[site][1] )
            html+='</ul>'
            
    time_point("Input checked")


    html += """
<hr><br>
<a name=ERROR></a>
<ul>
<li> <b><i>dashboard numbers over %d days</b></i>
<li> &uarr; %% with respect to total number of error in the code
<li> &rarr; %% with respect to total number of error at the site
</ul>
"""%(dash_board_h)

    html += '<br>'

    n_expose_base = options.expose# if options else UC.get('n_error_exposed')
    print "getting",n_expose_base,"logs by default"
    if tasks:
        min_rank = min([task.count('/') for task in tasks])
    for task in tasks:  
        n_expose = n_expose_base

        expose_archive_code = dict([(str(code), defaultdict(lambda : n_expose)) for code in UC.get('expose_archive_code')])
        expose_condor_code = dict([(str(code), defaultdict(lambda : n_expose)) for code in UC.get('expose_condor_code')])

        #print task
        task_rank = task.count('/')
        task_short = task.split('/')[-1]
        total_per_site = defaultdict(int)
        time_point("Starting with task %s"% task_short, sub_lap=True)
        
        notreported='NotReported'
        
        total_count= defaultdict(int)
        error_site_count = defaultdict( lambda : defaultdict(int))

        all_not_reported = set()
        for agent in stat['AgentJobInfo']:
            for site in stat['AgentJobInfo'][agent]['tasks'].get(task,{}).get('skipped',{}):
                info = stat['AgentJobInfo'][agent]['tasks'][task]['skipped'][site]
                #print info
                all_not_reported.add( site )
                ce = SI.SE_to_CE( site )
                error_site_count[notreported][ce] += info.get('skippedFiles',0)
                total_count[notreported] += info.get('skippedFiles',0)

            for site in stat['AgentJobInfo'][agent]['tasks'].get(task,{}).get('sites',{}):

                info = stat['AgentJobInfo'][agent]['tasks'][task]['sites'][site]
                for s in ['success','failure','cooloff','submitted']:
                    if not s in info: continue
                    data = info[s]
                    if type(data)==dict:
                        total_per_site[site] += sum( data.values() )
                    else:
                        total_per_site[site] += data

        #is the task relevant to recover (discard log, cleanup)
        if any([v in task.lower() for v in ['logcol','cleanup']]): continue


        #total_count= defaultdict(int)
        #error_site_count = defaultdict( lambda : defaultdict(int))
        if not task in err:
            print task,"has not reported error"
            err[task] = {}
        #print err[task].keys()
        
        for exittype in err[task]:
            #print "\t",err[task][exittype].keys()
            for errorcode_s in err[task][exittype]:
                if errorcode_s == '0' : continue
                #print "\t\t",err[task][exittype][errorcode_s].keys()
                for site in err[task][exittype][errorcode_s]:
                    ce = SI.SE_to_CE(site)
                    count = err[task][exittype][errorcode_s][site]['errorCount']
                    total_count[errorcode_s] += count
                    #error_site_count[errorcode_s][site] += count
                    error_site_count[errorcode_s][ce] += count

        ## show the total
        all_sites = set()
        all_codes = set()
        for code in error_site_count:
            for site in error_site_count[code]:
                all_sites.add( site )
                if code != '0':
                    all_codes.add( code)

        s_per_code =defaultdict(int)
        for site in all_sites:
            for code in sorted(all_codes):
                s_per_code[code] += error_site_count[code][site]
        
        expose_top_N = UC.get('expose_top_N')
        count_top_N = min( sorted(s_per_code.values(), reverse=True)[:expose_top_N]) if s_per_code else -1

        
        for exittype in err[task]:
            #print "\t",err[task][exittype].keys()
            for errorcode_s in err[task][exittype]:
                if errorcode_s == '0' : continue
                #print "\t\t",err[task][exittype][errorcode_s].keys()
                force_code = (count_top_N>0 and s_per_code[errorcode_s] >= count_top_N)
                if force_code: print "will expose",errorcode_s,"anyways"
                for site in err[task][exittype][errorcode_s]:
                    ce = SI.SE_to_CE(site)
                    count = err[task][exittype][errorcode_s][site]['errorCount']
                    ###total_count[errorcode_s] += count
                    #error_site_count[errorcode_s][site] += count
                    ###error_site_count[errorcode_s][ce] += count
                    for sample in err[task][exittype][errorcode_s][site]['samples']:
                        #print sample.keys()
                        for step in sample['errors']:
                            for report in  sample['errors'][step]:
                                if report['type'] == 'CMSExeption': continue
                                #if int(report['exitCode']) == int(errorcode_s):
                                one_explanation[errorcode_s].add("%s (Exit code: %s) \n%s"%(report['type'], report['exitCode'], report['details']))
                                per_task_explanation["%s:%s"%(task_short,errorcode_s)].add("%s (Exit code: %s) \n%s"%(report['type'], report['exitCode'], report['details']))
                                #one_explanation[errorcode_s].add( report['details'] )
                                #else:
                                #one_explanation[
                        agent = sample['agent_name']
                        wmbs = sample['wmbsid']
                        workflow = sample['workflow']
                        if force_code:
                            if not errorcode_s in expose_condor_code:
                                expose_condor_code[errorcode_s] = defaultdict(lambda : n_expose)
                            if not errorcode_s in expose_archive_code:
                                expose_archive_code[errorcode_s] = defaultdict(lambda : n_expose)

                        if do_CL and ((errorcode_s in expose_condor_code and expose_condor_code[errorcode_s][agent])) and 'cern' in agent:
                            if errorcode_s in expose_condor_code:
                                expose_condor_code[errorcode_s][agent]-=1
                            print errorcode_s,agent,"error count",expose_condor_code.get(errorcode_s,{}).get(agent,0)

                            threads.append(AgentBuster( agent =agent, 
                                                        workflow = workflow, 
                                                        wmbs = wmbs, 
                                                        errorcode_s = errorcode_s, 
                                                        base_eos_dir = base_eos_dir, 
                                                        monitor_eos_dir = monitor_eos_dir, 
                                                        task_short = task_short))

                        for out in sample['output']:
                            #print out
                            if out['type'] == 'logArchive':
                                if do_JL and ((errorcode_s in expose_archive_code and expose_archive_code[errorcode_s][agent]>0)):
                                    if errorcode_s in expose_archive_code:
                                        expose_archive_code[errorcode_s][agent]-=1
                                    print errorcode_s,agent,"error count",expose_archive_code.get(errorcode_s,{}).get(agent,0)

                                    threads.append( XRDBuster(
                                                              out_lfn = out['lfn'],
                                                              monitor_eos_dir = monitor_eos_dir,
                                                              wfn = wfn,
                                                              errorcode_s = errorcode_s,
                                                              task_short = task_short,
                                                              from_eos = (not options.not_from_eos),# if options else True),
                                                              ) )

        #print task
        #print json.dumps( total_count, indent=2)
        #print json.dumps( explanations , indent=2)
        all_sites = set()
        all_codes = set()
        for code in error_site_count:
            for site in error_site_count[code]:
                all_sites.add( site )
                if code != '0':
                    all_codes.add( code)

        ## parse the dashboard data
        for site in total_by_site_dash:
            ## no. cannot discriminate by task in dashboard...
            #all_sites.add( site )
            pass

        ## parse the acdc data
        #notreported='NotReported'
        #all_missing_stats = set()
        #for site in missing_to_run_at[task] if task in missing_to_run_at else []:
        #    if not missing_to_run_at[task][site]: continue
        #    ce = SI.SE_to_CE( site )
        #    #all_sites.add( ce )
        #    all_missing_stats.add( ce )

            
        #all_missing_stats = all_missing_stats &set(SI.all_sites)
        #all_not_reported = all_missing_stats - all_sites 
        #print task
        #print "site with no report",sorted(all_not_reported)
        #print sorted(all_sites)
        #print sorted(all_missing_stats)
        #all_sites = all_missing_stats | all_sites


        #all_sites = all_sites & set(SI.all_sites)

        no_error = len(all_not_reported)!=0
        
        if not no_error and notreported in all_codes:
            all_codes.remove( notreported )
        missing_events = missing_to_run[task] if task in missing_to_run else 0
        feff = wfi.getFilterEfficiency( task.split('/')[-1] )
        html += "<a name=%s>"%task.split('/')[-1]
        html += "<b>%s</b>"%task.split('/')[-1]
        if missing_events:
            if feff != 1.:
                html += ' is missing %s events in input and <b>about %s events in output</b>'%( "{:,}".format(missing_events),
                                                                                                "{:,}".format(int(missing_events*feff)))
            else:
                html += ' is missing <b>%s events in I/O</b>'%( "{:,}".format(missing_events))

            html += ' <a href="https://cmsweb.cern.ch/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key=%%22%s%%22&include_docs=true&reduce=false" target=_blank>AC/DC</a>'%( wfn )
            if no_error:
                html +="<br><b><font color=red> and has UNreported error</font></b>"


        html += "<br><table border=1><thead><tr><th>Sites/Errors</th>"

        #for site in all_sites:
        #    html+='<th>%s</th>'%site
        for code in sorted(all_codes):
            #html+='<th><a href="#%s">%s</a>'%(code,code)
            html+='<th><a href="#%s:%s">%s</a>'%(task_short,code,code)
            if (str(code) in expose_archive_code or do_all_error_code):# and n_expose_base:
                html += ' <a href=%s/joblogs/%s/%s/%s>, JobLog</a>'%( unified_url_eos, wfn, code, task_short )
            if (str(code) in expose_condor_code or do_all_error_code):# and n_expose_base:
                html += ' <a href=%s/condorlogs/%s/%s/%s>, CondorLog</a>'%( unified_url_eos, wfn, code, task_short )
            html += '</th>'

        html+='<th>Total jobs</th><th>Site Ready</th>'
        html+='</tr></thead>\n'

        html+='<tr><td>Total</td>'
        for code in sorted(all_codes):
            html += '<td bgcolor=orange width=100>%d'%(s_per_code[code])
            if code in total_by_code_dash:
                html += ' (<b><i>%d</i></b>)'% total_by_code_dash[code]
            html += '</td>'

        ulist='<ul>'
        grand=0
        for status in sorted(status_per_task[task].keys()):
            ulist+='<li> %s %d'%( status, status_per_task[task][status])
            grand+= status_per_task[task][status]
        ulist+='<li><b> Total %d </b>'%grand
        ulist+='</ul>'
        #html += '<td bgcolor=orange> %.2f%% </td>'% (100.*(float(sum(s_per_code.values()))/sum(total_per_site.values())) if sum(total_per_site.values()) else 0.)
        html += '<td bgcolor=orange> &rarr; %.2f%% &larr; </td>'% (100.*(float(sum(s_per_code.values()))/ grand) if grand else 0.)
        html += '<td bgcolor=orange> %s </td>'% ulist
        
        html+='</tr>'


        def palette(frac):
            _range = { 
                0.0 : 'green',
                0.5 : 'green',
                0.6 : 'darkgreen',
                0.7 : 'orange',
                0.8 : 'salmon',
                0.9 : 'red'
                }
            which = [k for k in _range.keys() if k<=frac]
            if which:
                there = max(which)
            else:
                there=max(_range.keys())
            return _range[there]

        for site in sorted(all_sites):
            site_in = 'Yes'
            color = 'bgcolor=lightblue'
            if not site in SI.sites_ready:
                color = 'bgcolor=indianred'
                site_in ='<b>No</b>'
                if task in missing_to_run_at and  missing_to_run_at[task][SI.CE_to_SE(site)] == 0 or min_rank == task_rank:
                    color = 'bgcolor=aquamarine'
                    site_in = '<b>No</b> but fine'

            if not no_error:
                site_in +=" (%s events)"%("{:,}".format(missing_to_run_at[task][SI.CE_to_SE(site)]) if task in missing_to_run_at else '--')
            html+='<tr><td %s>%s</td>'%(color,site)
            for code in sorted(all_codes):
                if code == notreported:
                    html += '<td %s width=200>%s events </td>' %(color, "{:,}".format(missing_to_run_at[task][SI.CE_to_SE(site)]))
                else:
                    if error_site_count[code][site]:
                        er_frac = float(error_site_count[code][site])/s_per_code[code] if s_per_code[code] else 0.
                        si_frac = float(error_site_count[code][site])/total_per_site[site] if total_per_site[site] else 0.
                        html += '<td %s width=200>%d'%(color, error_site_count[code][site])
                        if code in r_dashb and site in r_dashb[code]:
                            html += ' (<b><i>%d</i></b>)'%( r_dashb[code][site] )

                        html += ', <font color=%s>&uarr; %.1f%%</font>, <font color=%s>&rarr; %.1f%%</font></td>'% (
                            palette(er_frac),100.*er_frac,
                            palette(si_frac), 100.*si_frac
                            )
                    else:
                        html += '<td %s>0</td>'% color
            html += '<td bgcolor=orange>%d</td>'% total_per_site[site]
            html += '<td %s>%s</td>'% (color, site_in)
            html +='</tr>\n'
        html+='</table><br>'
        task_error_site_count[task] = error_site_count

    ## run all retrieval
    run_threads = ThreadHandler( threads = threads, n_threads = options.log_threads,# if options else 5,
                                 sleepy = 10, 
                                 timeout=UC.get('retrieve_errors_timeout'),
                                 verbose=True)
    run_threads.start()

    html += '<hr><br>'
    html += '<a name=BLOCK></a>'
    html += "<b>Blocks (%d/%d) needed for recovery</b><br>"%( len(needed_blocks_loc), len(all_blocks))
    for block in sorted(needed_blocks_loc.keys()):
        html +='%s <b>@ %s</b><br>'%(block, ','.join(sorted(needed_blocks_loc[block])))

    html += '<a name=FILE></a>'
    html += "<br><b>%s Files in no block</b><br>"%( len(files_and_loc_notin_dbs.keys()))
    rthreads = []
    check_files = [ f for f in files_and_loc_notin_dbs.keys() if '/store' in f]
    random.shuffle( check_files )
    #check_files = check_files[:100]
    check_files = []
    by_f = {}
    f_locations = defaultdict(set)
    if check_files:
        import dynamoClient
        DC=dynamoClient.dynamoClient()
        dirs_by_site = defaultdict(set)
        for f in check_files:
            dir,fn = f.rsplit('/',1)
            for s in files_and_loc_notin_dbs[f]:
                dirs_by_site[s].add( dir )
        files_by_site = DC.files_in_dir( dirs_by_site )
        #print dirs_by_site
        #print files_by_site
        
        for f in check_files:
            locs = [s for s in files_by_site if f in files_by_site[s] ]
            if locs:
                by_f[f] = True
                f_locations[f].update( locs )
            else:
                by_f[f] = False

        """
        for f in check_files:
            rthreads.append( ReadBuster( file = f ))
        print "checking on existence of",len(rthreads),"files"
        run_rthreads = ThreadHandler( threads = rthreads, n_threads = 20, timeout = 10)
        run_rthreads.start()
        while run_rthreads.is_alive():
            time.sleep(10)

        for t in run_rthreads.threads:
            by_f[t.file] = t.readable
            #print "checked",t.file,t.readable
        """
    files_html = ""
    existing_html = ""
    lost_html = ""
    separate_h = False
    missing_files = defaultdict(int)
    expected_files = defaultdict(int)
    max_number_of_files = 500 
    display_files = sorted(files_and_loc_notin_dbs.keys())
    display_files = display_files[:max_number_of_files] if max_number_of_files else display_files
    
    for f in display_files:
        readable = by_f.get(f,-1)
        if readable == -1 or not 'store' in f:
            fs = '%s'%f
            sites_strs = sorted(files_and_loc_notin_dbs[f])
        else:
            for s in files_and_loc_notin_dbs[f]:
                expected_files[s]+=1
            if readable == True:
                fs = '<font color="light green">%s</font>'%f
                #print f,"is readable"
            else:
                fs = '<font color=red>%s</font>'%f
                #print f,"is not readable"
                for s in files_and_loc_notin_dbs[f]:
                    missing_files[s]+=1
        
            sites_strs = [ '<font color="%s">%s</font>'% ('light green' if s in f_locations[f] else 'red', s) for s in sorted(files_and_loc_notin_dbs[f])]
            #seen_at = sorted(f_locations[f])
            
        html_line ='%s <b>@</b> %s<br>'%( fs, 
                                      ','.join( sites_strs ), 
                                      #','.join(seen_at)
                                  )
        if not separate_h:
            files_html += html_line
        if readable == False:
            lost_html += html_line
        else:
            existing_html += html_line
    html += "<br><table border=1><thead><tr><td>Site</td><td>Expected files</td><td>Missing files</td></tr></thead>"
    for s in sorted(expected_files.keys()):
        if missing_files[s] or True:
            html+="<tr bgcolor=%s><td>%s</td><td>%d</td><td>%d</td></tr>"%( "red" if missing_files[s] else "", s, expected_files[s], missing_files[s])
    html += "</table><br>"

    if separate_h:
        html += existing_html
        html += lost_html
    else:
        html += files_html

    html += '<hr><br>'
    html += '<a name=CODES></a>'
    html += '<table border=1>'
    for code in per_task_explanation:
        html +='<tr><td><a name="%s">%s</a><br><a href=https://twiki.cern.ch/twiki/bin/view/CMSPublic/JobExitCodes>code twiki</a></td><td>%s</td></tr>'% ( code, code, '<br><br>'.join(per_task_explanation[code]).replace('\n','<br>' ))
    #for code in one_explanation:
    #    html +='<tr><td><a name="%s">%s</a></td><td>%s</td></tr>'% ( code, code, '<br><br>'.join(one_explanation[code]).replace('\n','<br>' ))

    html+='</table>'
    html+=('<br>'*30)
    html +='</html>'
    time_point("Report finished")
    wfi.sendLog( 'error', html, show=False)
    fn = '%s'% wfn

    time_point("error send to ES")
    #open('%s/report/%s'%(monitor_dir,fn),'w').write( html )
    #open('%s/report/%s'%(monitor_eos_dir,fn),'w').write( html )
    #eosFile('%s/report/%s'%(monitor_dir,fn),'w').write( html ).close()
    eosFile('%s/report/%s'%(monitor_eos_dir,fn),'w').write( html ).close()

    time_point("Finished with showError")

    ## then wait for the retrivals to complete
    ping = 0
    while run_threads.is_alive():
        ping+=1
        if ping%100:
            time_point("waiting for sub-threads to finish")
        time.sleep(30)

    time_point("Finished with retrieval threads")

    return task_error_site_count, one_explanation


def parse_ongoing(url, options):
    parse_many(url, options, statuses= ['away'])

def parse_manual(url, options):
    parse_many(url, options, statuses= ['manual'])

def parse_many(url, options, statuses):
    wfos = []
    for s in statuses:
        if not s: continue
        wfos.extend(session.query(Workflow).filter(Workflow.status.contains(s)).all())
    random.shuffle( wfos ) 
    parse_those(url, options, [wfo.name for wfo in wfos])
        
        
def parse_all(url, options=None):
    those = session.query(Workflow).filter(Workflow.status == 'assistance-manual').all()
    parse_those(url, options, [wfo.name for wfo in those])

def condensed( st_d ):
    failure = sum(st_d.get('failure',{}).values())
    cooloff = sum(st_d.get('cooloff',{}).values())
    running = st_d.get('submitted',{}).get('running',0)
    pending = st_d.get('pending',0)
    success = st_d.get('success',0)
    queued = sum(st_d.get('queued',{}).values())
    created = failure + cooloff + running + pending + success + queued
    return {
        'created' : created,
        'queued' : queued,
        'success' : success,
        'failure' : failure,
        'cooloff' : cooloff,
        'running' : running,

        #'cooloff_f' : cooloff / float(success+1.),
        #'failure_f' : failure / float(success+1.)
        }
def ratios( ti ):
    return {
        'cooloff_f' :ti.get('cooloff',0) / float(ti.get('success',0)+1),
        'failure_f' :ti.get('failure',0) / float(ti.get('success',0)+1)
        }

def add_condensed( d1, d2):
    all_keys = set(d1.keys()+d2.keys())
    d3= {}
    for k in all_keys:
        d3[k] = d1.get(k,0)+d2.get(k,0)
    return d3

def parse_top(url, options=None):
    UC = unifiedConfiguration()
    top_N = UC.get('full_report_top_N')
    diagnose_by_agent_by_site = defaultdict( lambda : defaultdict( lambda : defaultdict(dict)))
    wm = dataCache.get('wmstats')

    for wfn in wm.keys():
        ## filter by runn*
        if not wm[wfn]['RequestStatus'] in ['running-open','running-closed',
                                            #'completed',
                                            ]: 
            continue
        info = wm[wfn].get('AgentJobInfo',{})
        for agent,ai in info.items():
            an = agent.split('.')[0]
            for task,ti in ai['tasks'].items():        
                for site,si in ti.get('sites',{}).items():
                    ssi = condensed( si )
                    diagnose_by_agent_by_site[task][an][site] = ssi 
               
    diagnose = defaultdict( dict ) ## the overall picture of the task
    diagnose_by_site = defaultdict( lambda : defaultdict(dict))
    diagnose_by_agent = defaultdict( lambda : defaultdict(dict))    

    for task in diagnose_by_agent_by_site:
        if '_ACDC' in task: continue
        if '_RVCMSSW' in task: continue
        for agent in diagnose_by_agent_by_site[task]:
            for site in diagnose_by_agent_by_site[task][agent]:
                diagnose_by_site[task][site] = add_condensed( diagnose_by_site[task][site], diagnose_by_agent_by_site[task][agent][site])
                diagnose_by_agent[task][agent] = add_condensed( diagnose_by_agent[task][agent], diagnose_by_agent_by_site[task][agent][site])
                diagnose[task] = add_condensed( diagnose[task], diagnose_by_agent_by_site[task][agent][site])


    ##include fractions
    for t,ti in diagnose.items():
        diagnose[t].update( ratios(ti) )
    for t in diagnose_by_site:
        for s,ti in diagnose_by_site[t].items():
            diagnose_by_site[t][s].update( ratios(ti) )
    for t in diagnose_by_agent:
        for a,ti in diagnose_by_agent[t].items():
            diagnose_by_agent[t][a].update( ratios(ti) )
    for t in diagnose_by_agent_by_site:
        for a in diagnose_by_agent_by_site[t]:
            for s,ti in diagnose_by_agent_by_site[t][a].items():
                diagnose_by_agent_by_site[t][a][s].update( ratios(ti) )

    #top_cooloff = dict(sorted([(t,i.get('cooloff',0)) for t,i in diagnose.items()], key = lambda o:o[1], reverse=True)[:top_N])
    #top_failure = dict(sorted([(t,i.get('failure',0)) for t,i in diagnose.items()], key = lambda o:o[1], reverse=True)[:top_N])
    top_cooloff = dict(sorted([(t,i.get('cooloff_f',0)) for t,i in diagnose.items()], key = lambda o:o[1], reverse=True)[:top_N])
    top_failure = dict(sorted([(t,i.get('failure_f',0)) for t,i in diagnose.items()], key = lambda o:o[1], reverse=True)[:top_N])
    
    all_bad_wfs = set()
    all_bad_wfs.update([t.split('/')[1] for t in top_cooloff.keys()] )
    all_bad_wfs.update([t.split('/')[1] for t in top_failure.keys()] )
    
    print "found",len(all_bad_wfs),"to parse for detailled error report"
    parse_those(url, options, all_bad_wfs)

    #ht = open('%s/toperror.html'%monitor_eos_dir, 'w')
    ht = eosFile('%s/toperror.html'%monitor_eos_dir, 'w')
    ht.write("""<html>
Report of workflows with top %s error in failure and cooloff<br>
Last updated on %s (GMT)

<table border=1>
<thead><tr><th>Workflow</th><th>Task</th><th>Success</th><th>Failures</th><th>Fail.Frac</th><th>Cooloffs</th><th>Cool.Frac</th><th> Task Error Report </th> </tr></thead>
"""%(
            top_N,
            time.asctime( time.gmtime())
            ))

    ## sort by max errors 
    tops = defaultdict(int)
    for wf in sorted(all_bad_wfs):
        for owf,N in top_cooloff.items():
            if wf in owf:
                tops[wf]+=N
        for owf,N in top_failure.items():
            if wf in owf:
                tops[wf]+=N

    for iw,(wf,count) in enumerate(sorted(tops.items(), key = lambda o : o[1], reverse=True)):
        report = set()
        for owf,N in top_cooloff.items():
            if wf in owf:
                report.add( owf)
        for owf,N in top_failure.items():
            if wf in owf:
                report.add( owf)
        
        lcol = 'bgcolor=%s'% ( 'white' if iw%2==0 else 'lightblue')
        for task in sorted(report):
            tdiag = diagnose.get(task,{})
            print task
            print tdiag
            ht.write('<tr %s><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n'%(lcol,
                                                                                                                               wf,
                                                                                              task.split('/')[-1],
                                                                                              tdiag.get('success','-'),
                                                                                              tdiag.get('failure','-'),
                                                                                              tdiag.get('failure_f','-'),
                                                                                              tdiag.get('cooloff','-'),
                                                                                              tdiag.get('cooloff_f','-'),
                                                                                              '<a href=%s/report/%s#%s> report </a>'%(unified_url,wf, task.split('/')[-1]))
                     )
    ht.write('</table></html>')
    ht.close()

def parse_those(url, options=None, those=[]):

    explanations = defaultdict(set)
    alls={}
    threads = []

    for wfn in those:
        threads.append( ParseBuster( url = url, wfn = wfn, options=options))


    #threads = thread[:5]
    run_threads = ThreadHandler( threads = threads, n_threads = options.threads,# if options else 5,
                                 sleepy = 10,
                                 timeout = None,
                                 verbose = True )

    print "running all workers"
    run_threads.start()
    while run_threads.is_alive():
        time.sleep(10)

    print "all threads completed"
    for worker in threads:
        task_error = worker.task_error
        one_explanation = worker.one_explanation
        alls.update( task_error )
        for code in one_explanation:
            explanations[code].update( one_explanation[code] )

    #open('%s/all_errors.json'%monitor_dir,'w').write( json.dumps(alls , indent=2 ))
    eosFile('%s/all_errors.json'%monitor_dir,'w').write( json.dumps(alls , indent=2 )).close()

    explanations = dict([(k,list(v)) for k,v in explanations.items()])

    #open('%s/explanations.json'%monitor_dir,'w').write( json.dumps(explanations, indent=2))
    eosFile('%s/explanations.json'%monitor_dir,'w').write( json.dumps(explanations, indent=2)).close()

    #alls = json.loads( open('all_errors.json').read())

    affected=set()
    per_code = defaultdict(set)
    for task in alls:
        for code in alls[task]:
            per_code[code].add( task.split('/')[1])
        
    for code in per_code:
        print code
        print json.dumps( sorted(per_code[code]), indent=2)



class showError_options(object):
    def __init__(self, **args):
        UC = unifiedConfiguration()
        self.default = {
            'no_JL' : { 'default' : False,
                        'action' : 'store_true',
                        'help' : 'Do not get the job logs'
                        },
            'no_CL' : { 'default' : False,
                       'action' : 'store_true',
                       'help' : 'Do not get the condor logs'
                       },
            'all_errors' : { 'default' : False,
                             'action' : 'store_true',
                             'help' : 'Bypass and expose all error codes'
                             },
            'cache' : { 'default': 0,
                        'type' : float,
                        'help' : 'The age in second of the error report before reloading them'
                        },
            'expose' : { 'default' : UC.get('n_error_exposed'),
                         'help' : 'Number of logs to retrieve',
                         'type' : int
                         },
            'not_from_eos' : { 'default' : False,
                               'action' : 'store_true',
                               'help' : 'Do NOT retrieve from job logs from eos'
                               },
            'threads' : { 'default' : 5,
                          'type' : int,
                          'help' : 'The number of parallel workers to get reports'
                          },
            'log_threads' : { 'default' : 3,
                              'type' : int,
                              'help' : 'The number of parallel workers to get logs per report'
                              },
                             
            }
        for opt,defv in self.default.items():
            setattr( self, opt, args.get(opt, defv['default']))
            
    def set_parser(self, parser):
        for opt,defv in self.default.items():
            parser.add_option('--%s'%opt, 
                              **defv)
    def from_parser(self, options):
        for opt,defv in self.default.items():
            setattr( self, opt, getattr(options,opt))

            
if __name__=="__main__":
    url = 'cmsweb.cern.ch'
    
    mlock = moduleLock(component='showError', locking = False)
    ml = mlock()

    UC = unifiedConfiguration()
    parser = optparse.OptionParser()

    so = showError_options()
    so.set_parser( parser )
    #parser.add_option('--no_JL',help="Do not get the job logs", action="store_true",default=False)
    #parser.add_option('--no_CL',help="Do not get the condor logs", action="store_true",default=False)
    parser.add_option('--fast',help="Retrieve from cache and no logs retrieval", action="store_true", default=False)
    parser.add_option('--ongoing',help="Retrieve for all ongoing", action="store_true", default=False)
    parser.add_option('--manual',help="Retrieve for all workflows needing help", action="store_true", default=False)
    parser.add_option('--top',help="Retrieve the top N offenders",action="store_true", default=False)
    parser.add_option('--from_status',help="The coma separated list of status keywords",default="")
    #parser.add_option('--cache',help="The age in second of the error report before reloading them", default=0, type=float)
    parser.add_option('--workflow','-w',help="The workflow to make the error report of",default=None)
    #parser.add_option('--expose',help="Number of logs to retrieve",default=UC.get('n_error_exposed'),type=int)
    #parser.add_option('--all_errors',help="Bypass and expose all error codes", default=False, action='store_true')
    #not used#parser.add_option('--no_logs',help="Bypass retrieval of logs", default=False, action='store_true')
    #parser.add_option('--from_eos',help="Retrieve from eos",default=False, action='store_true')
    #parser.add_option('--threads',help="The number of parallel workers to get report", default=5, type=int)
    #parser.add_option('--log_threads',help="The number of parallel workers to get logs per report", default=3, type=int)
    (options,args) = parser.parse_args()
    
    if options.fast:
        options.cache = 1000000
        options.no_JL = True
        options.no_CL = True


    so.from_parser( options )

    if options.workflow:
        parse_one(url, options.workflow, so)
    elif options.ongoing:
        parse_ongoing(url, so)
    elif options.manual:
        parse_manual(url, so)
    elif options.from_status:
        parse_many(url, so, statuses=options.from_status.split(','))
    elif options.top:
        parse_top(url, so)
    else:
        parse_all(url, so)

    print "ultimate",time.asctime(time.gmtime())
