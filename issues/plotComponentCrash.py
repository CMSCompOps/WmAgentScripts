import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import random
import re, sys
maxweek = 0
from pprint import pprint
def loadData(infile):
    #matrix (agent,component)
    crashByAgent = {}
    #matrix status,weeks
    crashByComponent = {}
    for line in open(infile).readlines():
        if not line:
            continue
        #read line
        (time, agent, component) = re.split('[\\t\\n]',line)[:3]
        #trim component to 8 chars when it's too large
        component = component[:8]
        if agent not in crashByAgent:
            crashByAgent[agent] = {}
        if component not in crashByAgent[agent]:
            crashByAgent[agent][component] = 0

        if component not in crashByComponent:
            crashByComponent[component] = {}
        if agent not in crashByComponent[component]:
            crashByComponent[component][agent] = 0

        crashByAgent[agent][component] += 1
        crashByComponent[component][agent] += 1
    pprint(crashByAgent)
    pprint(crashByComponent)
    return crashByAgent, crashByComponent



def generatePlot(data, keyset, title, xlabel, ylabel, filename):

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.grid(True)
    width = 3.8       # the width of the bars: can also be len(x) sequence
    N = len(keyset) #the number bars
    print "Number of bars!",N

    #sort keyset (bars)
    keyset = sorted(keyset)
    #sorted series
    series = sorted(data.keys())
    #make a matrix
    #fill empty slots with 0
    information = [ [(data[s][w] if w in data[s] else 0) for w in keyset] for s in series]
    width = 0.8      # the width of the bars: can also be len(x) sequence
    ind = np.arange(N) # the x locations for the groups
    
    bars = []
    maxy = 0
    # This is  the colormap I'd like to use.
    cm = plt.cm.get_cmap('Set1')
    for i in range(len(information)):
        #plot serie by serie
        information[i]
        #the bar must be as high as the sum of the previous rows to be stacked
        bottom = [sum(information[k][j] for k in range(i)) for j in range(N)]
        bar = plt.bar(ind+width/2, information[i], width, color=cm(i*15+20), edgecolor='white', bottom=bottom)
        bars.append(bar)
        maxy = max(b + v for b,v in zip(bottom, information[i]))
    
    
    plt.xlabel(xlabel)    
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(ind+width, keyset)
    plt.yticks(np.arange(0,maxy+2,max(maxy/5,1)))
    barcolors = [bar[0] for bar in bars]
    barcolors.reverse()
    plt.legend(barcolors, series, loc='best',prop={'size':8})
    #rotate labels and adjust space if too many    
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=45, size=10 )   
    plt.subplots_adjust(bottom=0.2)

    #plt.show()
    plt.savefig(filename)

def main():
    agent_log = sys.argv[1]
    (crashByAgent, crashByComponent) =  loadData(agent_log)
    generatePlot(crashByAgent,crashByComponent.keys(),'Crashes by Agent','agent','# of crashes', 'www/plots/by_agent.png')
    generatePlot(crashByComponent,crashByAgent.keys(),'Crashes by Component','component','# of crashes', 'www/plots/by_component.png')

if __name__ =="__main__":
    main()
