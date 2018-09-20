
"""
    Analyzes duplicate dump files and calculates the minimum file
    set for invalidating
    Usage: python analyzeDuplicates.py FILE
    FILE: A text file with the output of lumis from the duplicateEvents.py
    script
    Should be in this format:
      dataset : /DATASET_NAME
      runs
      lumi x is in these files
      file 1
      file 2
      lumi y is in these files
      file 3
      file 4
      ....
"""
import sys
import dbs3Client as dbs
import random
from optparse import OptionParser
import reqMgrClient

def buildGraphs(lines):
    """
    Builds a dictionary that contains an undirected graph for each
    dataset
    dataset -> graph.
    Each graph contains the representation of files with duplicate
    lumis
    Vertices: Files
    Edges: (f1,f2, #l) where f1 and f2 are two files that have
    duplicated lumis, and #l is the ammount of lumis they have
    in common
    """
    graphs = {}
    graph = None
    i = 0
    dataset = None
    while i < len(lines):
        #dataset name
        if "dataset" in lines[i]:
            dataset = lines[i].split(":")[-1].strip()
            graph = {}
            graphs[dataset] = graph
            i += 1
            continue
        #ignore o
        if( "Lumi" not in lines[i]
           and not lines[i].startswith("/") ):
            i += 1
            continue
        #text line 'Lumi ### is in the following files:'
        lumi = lines[i].split()[1]
        #text lines with file names
        f1 = lines[i+1]
        f2 = lines[i+2]
        #create edge (f1, f2)
        if f1 not in graph:
            graph[f1] = {}
        if f2 not in graph[f1]:
            graph[f1][f2] = 0
        graph[f1][f2] += 1
        #create edge (f2, f1)       
        if f2 not in graph:
            graph[f2] = {}
        if f1 not in graph[f2]:
            graph[f2][f1] = 0
        graph[f2][f1] += 1
        i += 3
    return graphs

def buildGraph(lumis):
    graph = {}
    
    for lumi in lumis:
        files = lumis[lumi]
        #text lines with file names
        f1 = files[0]
        f2 = files[1]
        #create edge (f1, f2)
        if f1 not in graph:
            graph[f1] = {}
        if f2 not in graph[f1]:
            graph[f1][f2] = 0
        graph[f1][f2] += 1
        #create edge (f2, f1)       
        if f2 not in graph:
            graph[f2] = {}
        if f1 not in graph[f2]:
            graph[f2][f1] = 0
        graph[f2][f1] += 1
    return graph

def getSumDegree(graph, v):
    """
    Returns the sum of all edges adjacent to the vertex.
    """
    return sum( e for v2, e in graph[v].items())

def hasEdges(graph):
    """
    True if at least one edge is between to vertices,
    that is, there is at least one lumi present in two different
    files
    """
    for v in graph.values():
        if v:
            return True
    return False

def deleteMaxDegreeFirst(graph, events):
    """
    Removes duplication by deleting files in a greedy fashion.
    That is, removing the files with the highest degree (duplicates)
    first, and keep doing so until there is no edge on the graph (no lumi
    in two different files)
    """
    files = []
    print "Initial files:", len(graph)
    #quadratic first
    while hasEdges(graph):
        maxv = None
        maxd = 0
        #get vertex with highest degree
        for v in graph:
            d = getSumDegree(graph,v)
            if d > maxd:
                maxd = d
                maxv = v
        #remove maxv from all its adjacent vertices
        for v in graph[maxv]:
            del graph[v][maxv]
        #remove maxv entry
        del graph[maxv]    
        files.append(maxv)
    
    #print "End Files:",len(graph), "Invalidated:",len(graph)
    return files

def deleteSmallestVertexFirst(graph, events):
    """
    Removes duplication by deleting files in a greedy fashion.
    That is, removing the files smallest files
    first, and keep doing so until there is no edge on the graph (no lumi
    in two different files)
    """
    files = []
    print "Initial files:", len(graph)
    #sort by number of events
    ls = sorted(graph.keys(), key=lambda x: events[x])
    #quadratic first
    while hasEdges(graph):
        #get smallest vertex
        minv = ls.pop()  
        #remove minv from all its adjacent vertices
        for v in graph[minv]:
            del graph[v][minv]
        #remove maxv entry
        del graph[minv]    
        files.append(minv)
    
    #print "End Files:",len(graph), "Invalidated:",len(graph)
    return files




def colorBipartiteGraph(graph, events):
    """
    Removes duplication by identifying a bipartite graph and removing
    the smaller side
    """
    red = set()
    green = set()

    for f1, f2d in graph.items():
        f1red = f1 in red
        f1green = f1 in green
        for f2 in f2d.keys():
            f2red = f2 in red
            f2green = f2 in green
            #both have no color
            if not(f1red or f1green or f2red or f1green):
                red.add(f1)
                green.add(f2)
            #some has two colors:
            elif (f1red and f1green) or (f2red and f2green):
                print "NOT BIPARTITE GRAPH"
                raise Exception("Not a bipartite graph, cannot use this algorithm for removing")
            #have same color
            elif (f1red and f2red) or (f1green and f2green):
                print "NOT BIPARTITE GRAPH"
                raise Exception("Not a bipartite graph, cannot use this algorithm for removing")

            #both are colored but different
            elif f1red != f2red and f1green != f2green:
                continue
            #color opposite
            elif f1red:
                green.add(f2)
            elif f1green:
                red.add(f2)
            elif f2red:
                green.add(f1)
            elif f2green:
                green.add(f1)
    #validate against the # of events of the files
    eventsRed = sum(events[f] for f in red)   
    eventsGreen = sum(events[f] for f in green)   
    if eventsRed < eventsGreen:
        return list(red)
    else:
        return list(green)

def getFileEvents(dataset, files):
    """
    Builds a dict files-> num events
    """
    eventCount = {}
    for f in files:
        evs = dbs.getEventCountDataSetFileList(dataset, [f])
        #evs = random.randrange(10000)
        eventCount[f] = evs
    return eventCount



url = 'cmsweb.cern.ch'
def main():
    
    usage = "python %prog [OPTIONS]"
    parser = OptionParser(usage)
    parser.add_option("-a", "--doall",dest="doall", action="store_true" , default=False, 
                      help="It will analyze all datasets of the workflow from the beginning. If this option is true,"\
                        " you should provide a workflow name or a list of them in the --file option.")
    parser.add_option("-f", "--file",dest="file", 
                      help="Input file with the contents of duplicateEvents.py (a list of lumis and files)."\
                      " If you are using the --doall option, it should contain a list of workflows instead")
    
    options, args = parser.parse_args()
    workflows = None
    #if we not doing all, input should be treated as list of lumis an files
    if not options.doall and options.file:
        lines = [l.strip() for l in open(options.file)]
        graphs = buildGraphs(lines)
    # if do all and input file
    elif options.doall and options.file:
        workflows = [l.strip() for l in open(options.file)]
    elif options.doall and not options.file:
        workflows = args
    else:
        parser.error("You should provide an input file with the output of duplicateEvents")

    # get the output datasets of the workflos and create the graph
    if workflows:
        datasets = []
        for wf in workflows:
            datasets += reqMgrClient.outputdatasetsWorkflow(url, wf);
        
        graphs = {}
        #analyze each dataset
        for dataset in datasets:
            dup, lumis = dbs.duplicateRunLumi(dataset, verbose="dict", skipInvalid=True)
            #print lumis
            graphs[dataset] = buildGraph(lumis)
            
    
    for dataset, graph in graphs.items():
        #look for datasetname
        print "Getting events per file"
        events = getFileEvents(dataset, graph.keys())
        try:
            #first algorithm that assumes bipartition        
            files = colorBipartiteGraph(graph, events)
        except Exception as e:
            #second, algorithm
            #files = deleteMaxDegreeFirst(graph, events)
            files = deleteSmallestVertexFirst(graph, events)
        
        total = dbs.getEventCountDataSet(dataset)
        invalid = dbs.getEventCountDataSetFileList(dataset, files)
    
        print 'total events %s'%total
        print 'invalidated files %s'%len(files)
        print 'invalidated events %s'%invalid
        if total:
            print '%s%%'%(float(total-invalid)/total*100.0)
        for f in sorted(files):
            print f

if __name__ == '__main__':
    main()

