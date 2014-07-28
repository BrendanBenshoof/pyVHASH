import networkx, underlay
import math, random
import matplotlib.pyplot as plt
import vhash_greedy as vhash
import time
import csv
import numpy as np
import chord

render_overlay_plot = False
bother_showing_results = False
count_overlay_only = True
"""
def generate_vhash_graph(nodes):
    n = float(len(nodes))
    TABLE_SIZE = 2*int(log(n)/log(log(n)))+2

    print "generating overlay", n
    locs = {}
    revlocs = {}
    overlay = networkx.DiGraph()
    overlay.add_nodes_from(nodes)
    for n in nodes:
        locs[n] = vhash.randPoint()
        revlocs[locs[n]] = n
    close_peers = {}
    far_peers = {}
    for n in nodes:
        others = nodes[:]
        others.remove(n)
        close_peers[n] = []
        for ploc in vhash.getShell(locs[n],map(lambda x: locs[x],others)):
            close_peers[n].append(revlocs[ploc])
        if len(close_peers[n]) < TABLE_SIZE:
            missing = TABLE_SIZE - len(close_peers[n])
            close_peers[n] += sorted(others,key=lambda x: vhash.dist(loc[n],loc[x]))[:missing]
    for n in nodes:
        far_peers[n] = []
    for n in nodes:
        for p in close_peers[n]:
            for p2 in close_peers[p]:
                if p2 not in far_peers[n] and p2 != p:
                    far_peers[n].append(p2)
        #if len(far_peers[n]) > TABLE_SIZE*TABLE_SIZE:
            #far_peers[n] = random.sample(far_peers[n],TABLE_SIZE*TABLE_SIZE)
    for n in nodes:
        for p in close_peers[n]:
            overlay.add_edge(n,p)
        for p in far_peers[n]:
            overlay.add_edge(n,p)
    return overlay
"""
def generate_optimized_vhash_graph(nodes,real,gens):
    min_short_peers = 3*2+1
    max_long_peers = min_short_peers*min_short_peers

    locs = {}
    revlocs = {}
    overlay = networkx.DiGraph()
    overlay.add_nodes_from(nodes)
    for n in nodes:
        locs[n] = vhash.randPoint()
        revlocs[locs[n]] = n
    close_peers = {}
    far_peers = {}
    for i in range(0,gens):
        close_peers = {}
        far_peers = {}
        for n in nodes:
            others = nodes[:]
            others.remove(n)
            close_peers[n] = []
            for ploc in vhash.getShell(locs[n],map(lambda x: locs[x],others)):
                close_peers[n].append(revlocs[ploc])

            if len(close_peers[n]) < min_short_peers:
                for p in sorted(others,key = lambda x: vhash.dist(locs[n],locs[x])):
                    if p in close_peers[n]:
                        continue
                    close_peers[n].append(p)
                    if len(close_peers[n]) >= min_short_peers:
                        break

        for n in nodes:
            far_peers[n] = []
        for n in nodes:
            for p in close_peers[n]:
                for p2 in close_peers[p]:
                    if p2 not in far_peers[n] and p2 != n:
                        far_peers[n].append(p2)
            if len(far_peers[n]) > max_long_peers:
                far_peers[n] = random.sample(far_peers[n],max_long_peers)

        for n in nodes:
            unit_distance_per_hop = sum([vhash.dist(locs[n], locs[x]) for x in close_peers[n]])
            unit_distance_per_hop /=sum([ float(underlay.hops(real,n,x)) for x in close_peers[n]])
            zero = [0.0]*len(locs[n])
            error_vector = zero[:]
            for p in close_peers[n]:
                latency = float(underlay.hops(real,n,p))
                ideal_length = latency*unit_distance_per_hop
                delta_vec = map(lambda x,y: min([y-x,vhash.space_size-(y-x)]), locs[p], locs[n])
                delta_dist = vhash.dist(zero,delta_vec)
                error_dist = 1.0+ideal_length-delta_dist
                error_delta = map(lambda x: error_dist*x/delta_dist,delta_vec)
                error_vector = vhash.vec_sum(error_vector, error_delta)
            del revlocs[locs[n]]
            locs[n] = tuple(vhash.vec_sum(locs[n],error_vector))
            revlocs[locs[n]] = n

    for n in nodes:
        for p in close_peers[n]:
            overlay.add_edge(n,p)
        for p in far_peers[n]:
            overlay.add_edge(n,p)

    if render_overlay_plot:
        xs = [locs[x][0] for x in nodes]
        ys = [locs[x][1] for x in nodes]
        plt.scatter(xs,ys)
        plt.show()


    return overlay

def get_real_hops(real_graph,overlay,A,B):
    path = networkx.shortest_path(overlay,A,B)
    steps = []
    total = 0
    if count_overlay_only:
        return len(path)
    for i in range(1,len(path)):
        steps.append((path[i-1],path[i]))
    for s in steps:
        total = total + underlay.hops(real_graph,s[0],s[1])
    return total




#networkx.draw_circular(chord_overlay)
#plt.show()

def runTrial(num, real_graph):

    hoplist = []


    chord_overlay = generate_optimized_vhash_graph(random.sample(real_graph.nodes(),num), real_graph, num/10)
    print "done generating overlay. Sampling"

    now = time.time()
    for i in range(0,10000):
        x = random.choice(chord_overlay.nodes())
        y = random.choice(chord_overlay.nodes())
        while(x==y):
            x = random.choice(chord_overlay.nodes())
            y = random.choice(chord_overlay.nodes())

        hoplist.append(get_real_hops(real_graph,chord_overlay,x,y))
    print time.time()-now
    if bother_showing_results:
        plt.hist(hoplist,bins=range(1,41))
        plt.title("Latency Distribution")
        plt.xlabel("Hops")
        plt.ylabel("Frequency")
        plt.show()
    #mean = np.mean(hoplist)
    #varience = np.std(hoplist)
    return hoplist



if __name__ == "__main__":
    print "generating underlay"
    real_graph = underlay.generate_underlay(1000)
    print "done a"
    with open("BAD_IDEA.csv","w+") as fp:
        vhash.setd(2) #if you change, change in peerlist code too
        writer = csv.writer(fp)
        for n in [100]:
            print "starting CHORD"
            writer.writerow([n,"CHORD"]+chord.runTrial(n, real_graph))
            print "starting VHASH"
            writer.writerow([n,"VHASH"]+runTrial(n, real_graph))

