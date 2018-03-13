import glob
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint 
import sys
import json
from datetime import datetime, timedelta
import time
import matplotlib.cm as cm

from collections import defaultdict 
from operator import itemgetter
import matplotlib.ticker as ticker
from utils import base_eos_dir

import os

fig = plt.subplots(figsize=(15, 6))
ax1 = plt.subplot(121)
ax2 = plt.subplot(122)


filenames = sorted(glob.glob('%s/*.restart.json'%(base_eos_dir)))
str=  ['WorkQueueManager', 'DBS3Upload', 'PhEDExInjector', 'JobAccountant', 'JobCreator' ,'JobSubmitter'  ,'JobTracker' ,'JobStatusLite' ,'JobUpdater' ,'ErrorHandler' ,'RetryManager' ,'JobArchiver' ,'TaskArchiver' ,'AlertProcessor' ,'AlertGenerator' ,'AnalyticsDataCollector' ,'AgentStatusWatcher' ,'ArchiveDataReporter']

filesDict = {}
failuresDict = defaultdict(lambda: defaultdict(dict))
for component in str:
    failuresDict[component]=0

count_files = 0
for files in filenames:
    filesDict[files] = 0
    try:
        with open(files) as f:
            obj = json.load(f)
            for component in str:
                if (obj['timestamp']):
                    sec= (obj['timestamp'])
                    days= sec//(60*60*24)
                    if component in obj["data"]:
                        print  (files, component,  len(obj["data"][component]), obj['timestamp'],  days, datetime.now())
                        failuresDict[files][component] = len(obj["data"][component])
                    else:
                        failuresDict[files][component] = 0
        count_files += 1
    except IOError as exc: #Not sure what error this is
        if exc.errno != errno.EISDIR:
            raise


filesDict = ["" for x in range(count_files)]
count=0
lengthComp=0
menStd=0
nfail=0
jet= plt.get_cmap('tab20b')

for files in filenames:
    colors = iter(jet(np.linspace(0,10,200)))
    lengthComp=0
    count+=1 
    for component in str:
        nfail=failuresDict[files][component]
        plt.bar(count, nfail, alpha = 0.5, bottom = lengthComp, align = 'center', color=next(colors), yerr = menStd, linewidth = 1)
        lengthComp += nfail
        filesDict[count-1]=files[:-21]
        plt.subplot(121) 
plt.legend(str, ncol=1)
plt.setp(plt.gca().get_legend().get_texts(), fontsize='10')
plt.ylabel("Frequency")
plt.xlim((0,count_files+6))
plt.plot(kind='bar', stacked=True);
start,end = ax1.get_xlim()
ax1.xaxis.set_ticks(np.arange(1, end, 1.0))
ax1.xaxis.set_major_formatter(ticker.FormatStrFormatter('%1.0f'))
ax1.set_xticklabels(filesDict, rotation='25', fontsize='8')
        

plt.subplot(122)
comp=0
jet= plt.get_cmap('tab20b')
for component in str: 
    lengthComp=0
    comp+=1
    colorss = iter(jet(np.linspace(0,10,200)))
    for files in filenames: 
        nfailcomp=failuresDict[files][component]
        #print (comp, component, files, failuresDict[files][component])
        plt.bar(comp, nfailcomp,  alpha=0.5, bottom = lengthComp, align='center', color=next(colorss), yerr=menStd, linewidth=1)
        lengthComp += nfailcomp
        plt.subplot(122)
    
plt.subplot(122) 
plt.legend(filesDict)
plt.ylabel("Frequency")
ax2.xaxis.set_ticks(np.arange(1, 19, 1.0))
ax2.xaxis.set_major_formatter(ticker.FormatStrFormatter('%1.0f'))
ax2.set_xticklabels(str, rotation='25', fontsize='8')

date_string = time.strftime("%d_%m_%Y")
plt.savefig('wmstat_'+date_string+'.png',dpi=100)     
#plt.show()


