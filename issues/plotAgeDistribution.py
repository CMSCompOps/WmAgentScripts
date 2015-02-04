"""
Creates a plot of age distribution.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import random, sys
from datetime import datetime
from pprint import pprint
try:
    import json
except ImportError:
    import simplejson as json
weekSet = set()
ignoreCampaign = set(['CMSSW_7_1_0','CMSSW_7_2_0_pre4','TEST'])


def getIndex(status):
    l = ["assignment-approved","acquired","running-open", "running-closed", "completed", "closed-out"]
    #print status
    return l.index(status)

def loadData(infile, jsonFile=False):
    #matrix type,weeks
    wfsByType = {}
    #matrix status,weeks
    wfsByStatus = {}
    #matrix campaign, weeks
    wfsByCampaign = {}
    
    if jsonFile:
        d = open(infile).read()
        #print d
        reqs = json.loads(d)
    else:
        reqs = open(infile).readlines()
    
    global weekSet
    for req in reqs:
        #skip empty
        if not req:
            continue
        
        #load json strings
        if jsonFile:
            name = req['requestname']
            status = req['status']
            campaign = req['campaign']
            days = req['requestdays']
            rtype = req['type']
            weeks = int(days/7)
        else:
            #read line
            (name, status, rtype, weeks, campaign) = req.split()
            weeks = int(weeks)
        #ignore anything that says "Test"
        if 'test' in name.lower() or 'test' in campaign.lower():
            continue
        #ignore anything that says "backfill"
        if 'backfill' in name.lower() or 'backfill' in campaign.lower():
            continue
        #ignore in closed out
        if status == 'closed-out':
            continue
        #ignore resubmissions:
        if rtype == 'Resubmission':
            continue
        #ignore relvals and test
        if campaign in ignoreCampaign:
            continue
        #ignore closed out?
        weekSet.add(weeks)
    
        #fill matrixes and count        
        if rtype not in wfsByType:
            wfsByType[rtype] = {}
        if weeks not in wfsByType[rtype]:
            wfsByType[rtype][weeks] = 0

        if status not in wfsByStatus:
            wfsByStatus[status] = {}
        if weeks not in wfsByStatus[status]:
            wfsByStatus[status][weeks] = 0

        if campaign not in wfsByCampaign:
            wfsByCampaign[campaign] = {}
        if weeks not in wfsByCampaign[campaign]:
            wfsByCampaign[campaign][weeks] = 0

        wfsByType[rtype][weeks] += 1
        wfsByStatus[status][weeks] += 1
        wfsByCampaign[campaign][weeks] += 1
        #TODO for debugging
        print name+'\t'+status+'\t'+rtype+'\t'+str(weeks)

    return wfsByType, wfsByStatus, wfsByCampaign

def generatePlot(data, keyset, title, xlabel, ylabel, filename, sortKey=None):
    #TODO for debugging
    pprint(data)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.grid(True)
    #the number of weeks/bars
    N = len(keyset)
     #sort keyset (bars)
    keyset = sorted(keyset)
    #sorted series
    series = sorted(set(data.keys()), key=sortKey)
    series.reverse()
    #fill empty slots with 0
    information = [ [(data[t][w] if w in data[t] else 0) for w in keyset] for t in series]

    ind = np.arange(N)    # the x locations for the groups
    width = 0.8       # the width of the bars: can also be len(x) sequence
    #for bottom we sum the ones we counted before
    cm = plt.cm.get_cmap('Set1')
    bars = []
    maxy = 0
    top = []
    for i in range(len(information)):
        #the bar must be as high as the sum of the following?previous rows to be stacked
        bottom = [sum(information[j][w] for j in range(i)) for w in range(N)]
        bar = plt.bar(ind+width/2, information[i], width, color=cm(i*12+20), edgecolor='white', bottom=bottom)
        bars.append(bar)
        top = [b + v for b,v in zip(bottom, information[i])]
        maxy = max(top)

    #create values in the top of the bar    
    autolabel(ind+width, top, ax)
    #axis
    plt.xlabel(xlabel)    
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(ind+width, keyset)
    plt.yticks(np.arange(0, maxy+1, max(maxy/5, 1)))
    series.reverse()
    barcolors = [bar[0] for bar in bars]
    barcolors.reverse()
    plt.legend(barcolors, series, loc='upper right',prop={'size':8})
    #plt.show()
    plt.savefig(filename)

def autolabel(xcoords, ycoords, ax):
    # attach some text labels
    for x, y in zip(xcoords,ycoords):
        ax.text(x, 1.05*y, '%d'%int(y),
                ha='center', va='bottom', fontsize=10 )

def main():
    wfsummary = sys.argv[1]
    jsonFile = False
    #if json read
    if len(sys.argv) > 2 and sys.argv[2] == '-j':
        jsonFile = True

    (wfsByType, wfsByStatus, wfsByCampaign) = loadData(wfsummary, jsonFile)
    now = datetime.now()
    generatePlot(wfsByType, weekSet, 'Distribution by Type',
        'weeks old (last updated on %s)'%(now.strftime("%Y-%m-%d")),
        '# of requests', './www/plots/by_type.png')
    generatePlot(wfsByStatus, weekSet, 'Distribution by Status',
        'weeks old (last updated on %s)'%(now.strftime("%Y-%m-%d")),
        '# of requests', './www/plots/by_status.png', sortKey=getIndex)
    generatePlot(wfsByCampaign, weekSet, 'Distribution by Campaign',
        'weeks old (last updated on %s)'%(now.strftime("%Y-%m-%d"))
        ,'# of requests', './www/plots/by_campaign.png')

if __name__ =="__main__":
    main()
