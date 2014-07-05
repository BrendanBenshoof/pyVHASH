import math
import random

d = 2

space_size = 100.0

def dist(p0,p1):
        deltas = map(lambda x,y: math.fabs(x-y),p0,p1)
        deltas = map(lambda x: x**2.0 if x < space_size else (space_size-x)**2.0, deltas)
        return sum(deltas)**0.5

def vec_sum(a,b):

        tmp_sum = map(lambda x,y: (x+y)%space_size, a,b)
        for i in range(0,len(a)):
                if tmp_sum[i] >= space_size:
                        tmp_sum[i] = space_size-tmp_sum[i]
                if tmp_sum[i] < 0:
                           tmp_sum[i] = space_size+tmp_sum[i]
        return tmp_sum

def randPoint():
        output = []
        for i in range(0,d):
                output.append(random.random()*space_size)
        return tuple(output)

def randSample(center,radius):
        thetas = []
        for i in range(0,d-1):
                thetas.append(2.0*math.pi*random.random())
        unit_vec = []
        for i in range(0,d):
                tmp = 1.0
                for j in range(0,i+1):
                        if j == i:
                                tmp*=math.cos(thetas[j-1])
                                break
                        else:
                                tmp*=math.sin(thetas[j-1])
                unit_vec.append(tmp)

        vec = map(lambda x: x*radius, unit_vec)
        return vec_sum(vec,center)

def voronoi_filter(objects,test_point):
        return min(objects,key=lambda x: dist(test_point,x))

def calc_voronoi_volume(center,others):
        radius = 50.0#max(map(lambda x: dist(center,x),others))
        peers = [center]+others
        total = 0
        for i in range(0,1000):
                p = randSample(center,radius*random.random())
                best = voronoi_filter(peers,p)
                if best == center:
                        total+=1
        return float(total)/radius**d

def getShell(center,others):
        others = sorted(others,key=lambda x: dist(center,x))
        peers = []
        peers.append(others[0])
        others.remove(others[0])
        area = calc_voronoi_volume(center,peers)
        for p in others:
                tmp_peers = peers+[p]
                tmp_area = calc_voronoi_volume(center,tmp_peers)
                if area - tmp_area > 0.00:
                        #print area - tmp_area
                        peers.append(p)
        return peers

points = []
center = [0.0]*d
for i in range(0,20):
        points.append(randPoint())
        print i,len(getShell(center,points))
