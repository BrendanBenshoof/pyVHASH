import math, random

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

def voronoi_filter(objects,test_point):
        return min(objects,key=lambda x: dist(test_point,x))

def randPoint():
        output = []
        for i in range(0,d):
                output.append(random.random()*space_size)
        return tuple(output)

def getShell(center,others):
    result = []
    for o in others:
        AB_half = map(lambda x,y: min([y-x,space_size-(y-x)])*0.5, center, o)
        midpoint = vec_sum(center,AB_half)
        tobeat = min([dist(center,midpoint),dist(midpoint,o)])
        best = True
        for othero in others:
            if o==othero:
                continue
            if dist(othero,midpoint) < tobeat:
                #print dist(othero,midpoint), tobeat
                best = False
                break
        if best:
            result.append(o)
    return result
"""
points = []
center = [0.0]*d
for i in range(0,20):
        points.append(randPoint())
        #print points
        print i,len(getShell(center,points))
"""