"""
    Analyzes duplicate dump files and calculates the minimum file
    set for invalidating
    Usage: python analyzeDuplicates.py DATASET FILE
    DATASET: the name of the dataset under analyzing
    FILE: A text file with the output of lumis from the duplicateEvents.py
    script
"""
import sys
import dbs3Client as dbs

def buildGraph(lines):
    """
    Builds an undirected graph interpretation of duplicates
    Vertices: Files
    Edges: (f1,f2, #l) where f1 and f2 are two files that have
    duplicated lumis, and #l is the ammount of lumis they have
    in common
    """
    graph = {}
    i = 0
    while i < len(lines):
        #ignore first lines
        if( not lines[i].startswith("Lumi")
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

def deleteMaxDegreeFirst(graph):
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

def colorBipartiteGraph(graph):
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
    #TODO validate against the # of events of the files
    if len(red) < len(green):
        return list(red)
    else:
        return list(green)

def main():
    dataset = sys.argv[1]
    lines = [l.strip() for l in open(sys.argv[2])]
    #build graph and calculate
    graph = buildGraph(lines)
    try:
        files = colorBipartiteGraph(graph)
    except Exception as e:
        files = deleteMaxDegreeFirst(graph)
    total = dbs.getEventCountDataSet(dataset)
    invalid = dbs.getEventCountDataSetFileList(dataset, files)

    print 'total events %s'%total
    print 'invalidated files %s'%len(files)
    print 'invalidated events %s'%invalid
    print '%s%%'%(float(total-invalid)/total*100.0)
    for f in files:
        print f

if __name__ == '__main__':
    main()

