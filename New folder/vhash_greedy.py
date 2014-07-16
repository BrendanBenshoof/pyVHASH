import math, random

d = 2

def setd(newd):
    global d
    d = newd

def getd():
    global d
    return d

space_size = 100.0

def dist(A,B):
    distance = 0.0
    for a,b in zip(A,B):
        ab = math.fabs(a-b)
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


def getShell(center, others):
    new_peers = []
    candidates =  sorted(others, key=lambda x: dist(center, x)) #sort candidates
    new_peers.append(candidates.pop(0))
    for c in candidates[:]:
        midpoint = calc_midpoint(center,c)  # find the midpoint between myself and the new point
        dist_to_midpoint = dist(center, midpoint)
        good = True
        for p in new_peers:  # now for each peer that I've added
            if dist(p, midpoint) < dist_to_midpoint: # if p is closer to the midpoint than I am...
                good = False
                break # reject it
        if good:
            candidates.remove(c)
            new_peers.append(c)
    if not False:
        while len(new_peers) < 3*d+1 and len(candidates) > 0:  #is this block nessecary
            new_peers.append(c)
    return new_peers


if __name__ ==  '__main__':
    """points = []
    center = [0.0]*d
    for i in range(0,1000):
        points.append(randPoint())
        #print points
        print i,len(getShell(center,points))"""
    a = [10, 5.0]
    b = [90, 5.0]
    print dist(a,b)
    print calc_midpoint(a,b)
