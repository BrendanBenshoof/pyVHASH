###
import math
import random
import vhash_greedy

d = 2

space_size = 100.0

def magnitude(p):
        zero = [0.0]*d
        return dist(zero,p)

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

def randSample(center):
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
        return unit_vec

def calc_radius(center, points, unit_vec):
        distances = []
        for p in points:
                AB = map(lambda x,y: min([y-x,space_size-(y-x)]), center, p)
                cos_theta = sum(map(lambda x,y: x*y, AB,unit_vec))/(magnitude(AB)*magnitude(unit_vec))
                theta = math.acos(cos_theta)
                dist = ((0.5*magnitude(AB))**2.0 + (0.5*magnitude(AB)*math.tan(theta))**2.0)**0.5
                distances.append(dist)
        return min(distances)


def approx_volume(center, points):
        radii = []
        for i in range(0,1000):
                unit = randSample([0.0]*d)
                radii.append(calc_radius(center, points, unit))
        return sum(radii)/float(len(radii))

def get_delunay_peers(center, points):
        points = sorted(points,key=lambda x: dist(center,x))
        peers = [points.pop(0)]
        vol = approx_volume(center, peers)
        for p in points:
                tmp_vol = approx_volume(center, peers+[p])
                if tmp_vol < vol:
                        vol = tmp_vol
                        peers.append(p)
        return peers

points = []
center = [0.0]*d
for i in range(0,20):
        points.append(randPoint())
        #print points
        print i,len(get_delunay_peers(center,points)),len(vhash_greedy.getShell(center,points))