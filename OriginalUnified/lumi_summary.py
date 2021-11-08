import json
import time
from utils import getWorkflowById, workflowInfo, getDatasetLumis, DbsApi, getWorkflowByOutput, monitor_dir
from collections import defaultdict
import pickle
import sys

url = 'cmsweb.cern.ch'


pid = 'ReReco-Run2015D-16Dec2015-0009'
if len(sys.argv) > 1:
    pid = sys.argv[1]

fetch=False
if len(sys.argv)>2:
    fetch = bool(int(sys.argv[2]))
    print "setting fetch to",fetch

wfs = getWorkflowById( url, pid , details=True)
if not wfs:
    print "no workflow for",pid
    sys.exit(-1)

in_dataset = None
input_json = defaultdict(list)
input_rl = []
output_json = {}
output_rl = defaultdict(list)
missing_rl = defaultdict(list)
errors_by_lb = defaultdict(lambda : defaultdict(set))
dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
ecode_ban = []#99999,139,134,92]

## try to get more workflows by their outputs
outs=set()
for wf in wfs:
    outs.update( wf['OutputDatasets'] )

for out in outs:
    o_wfs = getWorkflowByOutput( url , out, details=True )
    wfns = [wf['RequestName'] for wf in wfs]
    for o_wf in o_wfs:
        if not o_wf['RequestName'] in wfns:
            print "got also",o_wf['RequestName']
            wfs.append( o_wf )

for wf in wfs:
    wfi = workflowInfo( url, wf['RequestName'], request=wf)

    date = ''.join(map(lambda s : '%02d'%s ,wf['RequestDate']))

    if wf['RequestStatus'] not in ['completed','announced','normal-archived']: continue
    print "Checking on",wf['RequestName']

    ## create the input json
    if not input_json:
        in_dataset = wf['InputDataset']
        runs = wf['RunWhitelist']
        print "runs",",".join(map(str,runs)),"were processed"
        input_json = getDatasetLumis( in_dataset, runs=runs, with_cache=True)
        #print len(input_json)
        for r in input_json: input_rl.extend([(int(r),l) for l in input_json[r]])
        #print in_dataset,len(input_rl),"lumis processed",sorted( input_rl, key = lambda i:i[1])[-10:]

    ## collect the actual content of the output
    for out in wf['OutputDatasets']:
        if not out in output_json:
            #print out, (not fetch)
            output_json[out] = getDatasetLumis( out, with_cache=(not fetch))
            for r in output_json[out]:
                output_rl[out].extend([(int(r),l) for l in output_json[out][r]])
            #print out, len(output_rl[out]),sorted(output_rl[out], key = lambda i:i[1] )[-10:]

    ## make a diff ?
    for out in wf['OutputDatasets']:
        missing_rl[out] = list(set(input_rl) - set(output_rl[out]))
        if missing_rl[out]:
            print out,"is missing",len( missing_rl[out]),"lumisections"
            #print json.dumps( missing_rl[out], indent=2)
            pass

    print "now getting the summary for",wf['RequestName'],wf['RequestStatus']
    s=None
    while s==None:
        s = wfi.getSummary()
    if 'error' in s and s['error'] == 'not_found': 
        print s
        continue

    e=None
    #while e==None:
    #    e = wfi.getWMErrors()

    output_per_task = defaultdict(list)
    for ds in s['output']:
        for task in s['output'][ds]['tasks']:
            output_per_task[task].append( ds )
    #print json.dumps( output_per_task, indent =2 )

    #for task in e:
    #    if 'Cleanup' in task: continue
    #if 'Collect' in task: continue
    #    affected_outputs = set()
    #    for other_task in output_per_task:
    #        if other_task.startswith( task ):
    #            affected_outputs.update( output_per_task[other_task] )
    #    if not affected_outputs: continue
    #    print task,"affects",','.join(affected_outputs)
        

    #s={}## void the following 
    if 'errors' in s:
        errors = s['errors']
        #print 
        #print json.dumps( errors, indent=2 )
        for task in errors:
            if 'Cleanup' in task: continue

            #print json.dumps( list(output_per_task[task]), indent=2)
            affected_outputs = set()
            for other_task in output_per_task:
                if other_task.startswith( task ):
                    affected_outputs.update( output_per_task[other_task] )
            if not affected_outputs: continue
            print task,"affects",','.join(affected_outputs)

            for etype in errors[task]:
                print "\t",etype
                #if etype !='cmsRun1' : continue
                if not etype in ['cmsRun1','cmsRun2','stageOut1']: continue
                if type(errors[task][etype])!=dict: continue
                for ecode in errors[task][etype]:
                    #if ecode != '134': continue
                    #print "\t\t",ecode
                    if int(ecode) in ecode_ban: continue
                    eruns = []
                    details = ""
                    types = ""
                    for d in errors[task][etype][ecode]['errors']:
                        details = d['details'] 
                        types = d['type'] 



                    for run,ls in errors[task][etype][ecode]['runs'].items():
                        #print "JRJR",ls
                        ils=[]
                        for l in ls:
                            if type(l) == list:
                                #print l
                                #print list(range(l[0],l[1]+1))
                                ils.extend( range(l[0],l[1]+1) )
                            else:
                                ils.append( l ) 
                        eruns.extend( [(int(run),l) for l in ils] )
                    #print "in error",len(eruns)


                    #print "JRJR",eruns
                    #print "JRJR",set(eruns)
                    seruns = set(eruns)
                    affected = filter(lambda t: t[1], [(out,set(missing_rl[out])&seruns) for out in affected_outputs])
                    #print "affected",affected
                    for ds,affected_ls in affected:
                        for ls in affected_ls:
                            errors_by_lb[ds][ls].add( (date, str(task),str(etype),int(ecode),str(details),str(types)) )
    else:
        print "no errors for",wf['RequestName']
        #print s

#try:
#    open('/tmp/ls.%s.json'%pid,'w').write( json.dumps( missing_rl, indent=2 ))
#except:
#    print "Could not save the lumi json"

missing_rl_with_exp = {}
for out in missing_rl:
    missing_rl_with_exp[out] = {}    
    for ls in missing_rl[out]:
        missing_rl_with_exp[out][':'.join(map(str,ls))] = None



#sys.exit(1)

identified = {
#    50115 : '<font color=blue>Assert in EGamma</font>',
#    8028 : '<font color=green>Exception in PF</font>'
    }
ecode_minor = [99999,139,134,92]

print len(errors_by_lb)

for out in errors_by_lb:
    print out
    for ls in errors_by_lb[out]:
        print '\t',ls
        print '\t',len(errors_by_lb[out][ls]),"reasons in total"
        understood = False
        last = None
        last_reasons = []
        for reason in errors_by_lb[out][ls]:
            date, task,etype,ecode,details,types = reason
            if ecode in ecode_minor: continue
            if not last or date>last:
                last = date

        if last:
            for reason in errors_by_lb[out][ls]:
                date, task,etype,ecode,details,types = reason
                if ecode in ecode_minor: continue
                if date == last:
                    last_reasons.append( reason )
        else:
            ## need to go to minor errors
            for reason in errors_by_lb[out][ls]:
                date, task,etype,ecode,details,types = reason
                if not ecode in ecode_minor: continue
                if not last or date>last:
                    last = date
            for reason in errors_by_lb[out][ls]:
                date, task,etype,ecode,details,types = reason
                if not ecode in ecode_minor: continue
                if date == last:
                    last_reasons.append( reason )


        if last_reasons:
            print "latest reasons"
            rs=[]
            for reason in last_reasons:
                date, task,etype,ecode,details,types = reason
                #if ecode in identified
                print '\t\t',date, ecode, types
                print '\t\t',task
                #print '---\n',details,'\n---'
                if ecode in identified:
                    #rs.append( (identified[ecode],ecode) )
                    rs.append( "%s:%s"%(identified[ecode],ecode) )
                else:
                    rs.append( "%s:%s"%(types,ecode) )
            missing_rl_with_exp[out][':'.join(map(str,ls))] = "|".join(rs)

#if missing_rl_with_exp:                
## this one ism mandatory
#open('ls.%s.json'%pid,'w').write( json.dumps( missing_rl_with_exp, indent=2 ))
open('%s/datalumi/json/ls.%s.json'%(monitor_dir,pid),'w').write( json.dumps( missing_rl_with_exp, indent=2 ))
