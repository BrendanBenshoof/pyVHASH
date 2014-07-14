import math, random

d = 5

space_size = 100.0

def dist(A,B):
    distance = 0.0
    for a,b in zip(A,B):
        ab = math.sqrt((a-b)**2)
        if ab < (space_size - ab):
            distance = distance + (a-b)**2
        else:
            distance =  distance + (space_size - math.fabs(a-b))**2
    return math.sqrt(distance)

def calc_midpoint(A,B):
    midpoint = []
    for a,b in zip(A,B):
        c = 0
        ab = math.sqrt((a-b)**2)
        if ab < (space_size -ab):
            c = (a+b)/2.0
        else:
            c = ((a+b)%space_size)/2.0
        midpoint.append(c)
    return midpoint
    
    

def vec_sum(a,b):
        tmp_sum = map(lambda x,y: (x+y)%space_size, a,b)
        """
        for i in range(0,len(a)):
                if tmp_sum[i] >= space_size:
                        tmp_sum[i] = space_size-tmp_sum[i]
                if tmp_sum[i] < 0:
                           tmp_sum[i] = space_size+tmp_sum[i]
        """
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
        #AB_half = map(lambda x,y: min([y-x,space_size-(y-x)])*0.5, center, o)
        midpoint = calc_midpoint(center,o) #vec_sum(center,AB_half)
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


if __name__ ==  '__main__':
    """points = []
    center = [0.0]*d
    for i in range(0,1000):
        points.append(randPoint())
        #print points
        print i,len(getShell(center,points))"""
    a = [1, 5.0]
    b = [2, 5]
    print dist(a,b)
    print calc_midpoint(a,b)
