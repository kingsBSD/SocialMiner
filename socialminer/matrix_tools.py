from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt


from datetime import datetime

import networkx as nx
import numpy as np

__vsmall__ = 0.0001
__nearly1__ = 0.95

def clusterize(matrix,inflate=1.5):
    """Cluster an adjacency matrix by MCL: http://www.micans.org/mcl/"""
    start = datetime.now()
    
    mat = np.array(matrix)
    np.fill_diagonal(mat,1.0)
    mat = np.nan_to_num(mat / np.sum(mat,0))
    dim = len(matrix)

    iterations = 1
    converged = False

    while not converged:
        
        newMat = (np.dot(mat,mat)**inflate)
        newMat = np.nan_to_num(newMat / np.sum(newMat,0))                  
        diff = np.fabs(mat-newMat)
        dev = np.std(diff)
                
        output = 'Iteration: '+str(iterations)
        if dev > __vsmall__:
            output += " No convergence. Deviation: "+str(dev)
            mat = newMat
            iterations += 1
        else:
            output += ' Converged in '+str((datetime.now()-start).seconds)+ ' seconds.'
            converged = True
        print output
 
    labs = np.array(range(dim))
  
    clusterLists = [ list(j) for j in [ labs[newMat[i] > __vsmall__ ] for i in range(dim) ] if j.shape[0] > 2]

    clusterRef = {}
    for i, clust in enumerate(clusterLists):
        thisCluster = i+1
        for j in clust:
            clusterRef[j] = thisCluster
            
    return clusterLists, clusterRef

def labelClusters(clusters,labs):
    uniqueClusters = []
    clusterSets = {}
    for c in clusters:
        size = len(c)
        thisSet = set(c)
        setList = clusterSets.get(size,False)
        if setList:
            notThere = True
            for i in setList:
                if i == thisSet:
                    notThere = False
                    break
            if notThere:
                uniqueClusters.append(c)
                setList.append(thisSet)
        else:
            uniqueClusters.append(c)
            clusterSets[size] = [thisSet]
             
    return [ [ labs[i] for i in c ] for c in uniqueClusters if len(c) > 3 ]
        
def buildgraph(matrix,labels=False,clusters={},clustermode=False):
    
    if clustermode:
        clusterLabeler = lambda x: x
    else:
        clusterLabeler = lambda x: True
    
    G=nx.Graph()
    dim = len(matrix)
    
    if not labels:
        G.add_nodes_from(range(dim))
    else:
        for i,lab in enumerate(labels):
            G.add_node(i, label=lab)
            
    if clusters:
        for i in range(dim):
            cluster = clusters.get(i,False)
            if cluster:
                G.node[i]['cluster'] = clusterLabeler(cluster)
                
    for i in range(dim):
        G.add_edges_from([ (i,j) for j in range(dim) if matrix[i][j] ])

#    if clusters:
#        for i in range(dim):
#            for j in range(dim):
        
    return G

