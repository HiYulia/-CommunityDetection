 
from pyspark import SparkConf, SparkContext
import json
import sys
import time
import itertools
from collections import Counter
from itertools import chain
from collections import OrderedDict
import random
import collections
import math
from statistics import mean 

def getpairusers(business_users):
	return [tuple(sorted(list(x))) for x in itertools.combinations(business_users[1], 2)]
def single_source_shortest_path_basic(G, s):
	S = []
	P = {}
	for v in G:
		P[v] = []
	sigma = dict.fromkeys(G, 0.0)    # sigma[v]=0 for v in G
	D = {}
	sigma[s] = 1.0
	D[s] = 0
	Q = [s]
	while Q:   # use BFS to find shortest paths
		v = Q.pop(0)
		S.append(v)
		Dv = D[v]
		sigmav = sigma[v]
		for w in G[v]:
			if w not in D:
				Q.append(w)
				D[w] = Dv + 1
			if D[w] == Dv + 1:   # this is a shortest path, count paths
				sigma[w] += sigmav
				P[w].append(v)  # predecessors
	return S, P, sigma
def accumulate_edges(betweenness, S, P, sigma, s):
	delta = dict.fromkeys(S, 0)
	while S:
		w = S.pop()
		coeff = (1 + delta[w]) / sigma[w]
		for v in P[w]:
			c = sigma[v] * coeff
			if (v, w) not in betweenness:
				betweenness[(w, v)] += c
			else:
				betweenness[(v, w)] += c
			delta[v] += c
	return betweenness

def edge_betweenness(G):
	betweenness = dict.fromkeys(edgesset,0.0)
	nodes = G
	for s in nodes:
		S, P, sigma = single_source_shortest_path_basic(G, s)
		betweenness = accumulate_edges(betweenness, S, P, sigma, s)
	for i in betweenness:
		betweenness[i]=betweenness[i]/2  
	return betweenness 

def connected_components(G): 
	seen = set()
	for v in G:
		if v not in seen:
			c = set(set_bfs(G, v))
			yield c
			seen.update(c)            
def set_bfs(G, source):
	seen = set()
	nextlevel = {source}
	while nextlevel:
		thislevel = nextlevel
		nextlevel = set()
		for v in thislevel:
			if v not in seen:
				yield v
				seen.add(v)
				nextlevel.update(G[v])
def getModularity(S):
	Q = 0
	components_count=0
	for s in S:
		components_count+=1     
		edgesij = [tuple(sorted(list(x))) for x in itertools.combinations(s, 2)]
		for edge in edgesij:
			ki = nodeDegree[edge[0]]
			kj = nodeDegree[edge[1]]
			if edge in edgesset:
				Q += (1 - (ki * kj) / (2*m))
			else:
				Q += (0 - (ki * kj) / (2*m))
	return(Q/(2*m))

def task1(filter_threshold,input_file_path,betweenness_output_file_path,community_output_file_path):
	conf = SparkConf().setMaster("local").setAppName("HW4")
	sc=SparkContext(conf=conf)
	startTime = time.time()
	data=sc.textFile(input_file_path)
	header = data.first() 
	data = data.filter(lambda row:row != header)
	edges=data.map(lambda line: line.split(",")).map(lambda line: (line[1],line[0])).distinct().groupByKey()\
	.map(lambda e: (e[0],list(e[1]))).flatMap(lambda x: getpairusers(x)).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).\
	filter(lambda e: e[1]>=int(filter_threshold)).keys().persist()
	global edgesset,adjacency_lists,vertexs,nodeDegree,m
	edgesset=edges.collect()
	adjacency_lists=edges.flatMap(lambda x: ((x[0],x[1]),(x[1],x[0]))).groupByKey().map(lambda e: (e[0],list(e[1]))).collectAsMap()
	for u in adjacency_lists:
		adjacency_lists[u]={key: {} for key in adjacency_lists[u]}
	vertexs=list(adjacency_lists.keys())
	betweenness_result=edge_betweenness(adjacency_lists)
	betweenness_output= sc.parallelize(betweenness_result.items()).sortBy(lambda e: (-e[1],e[0])).collect()
	m=len(edgesset)
	nodeDegree = {}
	for node in vertexs:
		nodeDegree[node] = len(adjacency_lists[node])
	iterationdict=dict()
	betweenness_to_remove=betweenness_output
	for i in range(-1,int(m/5)):
		if i==-1:
			S=connected_components(adjacency_lists)
			FS=[]
			for s in S:
				FS.append(s)
			Modularityscore=getModularity(FS)
			iterationdict[Modularityscore]=FS
		else:
			edge_toremove=betweenness_to_remove[0][0]
			new_adjacency_lists=adjacency_lists
			del new_adjacency_lists[edge_toremove[0]][edge_toremove[1]]
			del new_adjacency_lists[edge_toremove[1]][edge_toremove[0]]
			new_betweenness_result=edge_betweenness(new_adjacency_lists)
			betweenness_to_remove= sc.parallelize(new_betweenness_result.items()).sortBy(lambda e: (-e[1],e[0])).collect()
			S=connected_components(new_adjacency_lists)
			newS=[]
			for s in S:
				newS.append(s)
			Modularityscore=getModularity(newS)
			iterationdict[Modularityscore]=newS
	
	max_modularity=max(list(iterationdict.keys()))
	Final_components=iterationdict[max_modularity]
	components_output=[]
	for thisc in Final_components:
		components_output.append(tuple(sorted(list(thisc))))
	components_output= sc.parallelize(components_output).map(lambda x:(x,len(x))).sortBy(lambda e: (e[1],e[0])).keys().collect()

	with open(betweenness_output_file_path, "w") as outfile1:
		for k in betweenness_output:
			i=', '.join(str(w) for w in k)
			outfile1.write(i+"\n")
	with open(community_output_file_path, "w") as outfile2:
		for k in components_output:
			i=', '.join("'"+str(w)+"'" for w in k)
			outfile2.write(i+","+"\n")
	endTime1 = time.time()
	time1=endTime1-startTime
	print("Duration " + str(time1))

if __name__ == "__main__":
		task1(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])

#print(result)

#spark-submit xinyue_niu_task1.py 7 sample_data.csv between.txt Community.txt