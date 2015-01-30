import networkx as nx
import matplotlib.pyplot as plt
import random
import triangle

def generate_grid_points(x):
	output = {}
	l = 1.0/float(x)
	for i in range(0,x):
		for j in range(0,x):
			id_num = j+i*x
			output[id_num] = (i*l,j*l)
	return output

def generate_random_points(x):
	output = {}
	for i in range(0,x):
		output[i] = (random.random(),random.random())
	return output

def dist(p0,p1):
	return sum(map(lambda x,y: (x-y)**2.0, p0,p1))**0.5

def mid(p0,p1):
	return map(lambda x,y: (x+y)/2.0,p0,p1)

def generate_graph(locs):
	g = nx.Graph()
	for k in locs.keys():
		g.add_node(k)
		g.node[k]['loc'] = locs[k]
	for k in locs:
		p = locs[k]
		for k1 in locs:
			p1 = locs[k1]
			if k1!=k:
				g.add_edge(k,k1,{'weight':dist(p,p1)})
	return g

def fast_delunay_prune(g):
	newG = nx.Graph()
	for n in g.nodes():
		newG.add_node(n)
		output = []
		l0 = g.node[n]['loc']
		canidates = sorted(g.neighbors(n),key=lambda x: g.edge[n][x]['weight'])
		output+=canidates[0:3]
		canidates = canidates[3:]
		extras = []
		for c in canidates:
			l1 = g.node[c]['loc']
			midpoint = mid(l0,l1)
			mid_dist = g.edge[n][c]['weight']/2.0
			success = True
			for p in output:
				if dist(midpoint,g.node[p]['loc']) < mid_dist:
					success = False
			if success:
				output.append(c)
			else:
				extras.append(c)
		#if len(output) < 7:
		#	output+= extras[:8-len(output)]
		for newlink in output:
			newG.add_edge(n,newlink)
	return newG


def true_deulunay(points):
	G = nx.Graph()
	for p in points.keys():
		G.add_node(p)
	triangles = triangle.delaunay([locs[x] for x in sorted(locs.keys())])
	for t in triangles:
		G.add_edge(t[0],t[1])
		G.add_edge(t[1],t[2])
		G.add_edge(t[2],t[0])
	return G

def graph_diff(g0,g1):
	m0 = nx.adjacency_matrix(g0)
	m1 = nx.adjacency_matrix(g1)
	delta = 0
	for i in range(0,m0.shape[0]):
		for j in range(0,m0.shape[1]):
			if i>j:
				#print m0[(i,j)],int(m1[(i,j)]>0)
				#if(m0[(i,j)]==1 and int(m1[(i,j)]>0)!=1):
				if(m0[(i,j)]!=int(m1[(i,j)]>0)):
					delta+=1
	return delta

ys = []
xs = [500,1000,2000,5000,100000]
for size in xs:

	locs = generate_random_points(size)


	G_truth = true_deulunay(locs)
	G_approx = generate_graph(locs)
	G_approx=fast_delunay_prune(G_approx)

	errors = graph_diff(G_truth,G_approx)
	print size, errors
	ys.append(errors)

plt.plot(xs,ys)
plt.show()
