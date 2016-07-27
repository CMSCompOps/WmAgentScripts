#!/usr/bin/env python
from assignSession import * 
from utils import sendEmail
from collections import defaultdict
import time

this_week = int(time.strftime("%W",time.gmtime()))
#start_time_two_weeks_ago = time.mktime(time.strptime("15-0-%d"%(this_week-3), "%y-%w-%W"))
now = time.mktime(time.gmtime())
start_time_two_weeks_ago = time.mktime(time.gmtime(now - (15*24*60*60))) # 14+1 days ago
last_week = int(time.strftime("%W",time.gmtime( now - (7*24*60*60))))
subject="Output Produced on Week %d"%(last_week)

ds=defaultdict(list)
n_produced=0
for out in session.query(Output).filter(Output.date>=start_time_two_weeks_ago).all():
    if  out.workflow.status in ['done-unlock','done','clean','clean-out','clean-unlock']:
        out_week = int(time.strftime("%W",time.gmtime(out.date)))
        if last_week==out_week:
            (_,_,_,tier) = out.datasetname.split('/')
            ds[tier].append( out.datasetname )
            n_produced+=1

text="""Dear all,

The folowing %d datasets have been produced last week:

"""%(n_produced)


for t in sorted(ds):
    text +="%d with datatier: %s \n"%( len(ds[t]), t)
    for d in sorted(ds[t]):
        text += "\t"+d+"\n"
    text += '\n'

text+="\n\n Regards,\nthe production team\n\nThis is an automated message."

sendEmail(subject, text, destination=['hn-cms-datasets@cern.ch'])
#sendEmail(subject, text)

