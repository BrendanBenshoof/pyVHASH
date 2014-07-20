import networkx, underlay
import math, random
import matplotlib.pyplot as plt

hash_space = 64
maxhash = 2**hash_space

def hash_dist(a,b):
    delta = b-a
    if delta < 0:
        return maxhash + delta
    return delta

def create_chord_graph(nodes):
    hashids = {}
    for n in nodes:
        hashids[n] = random.randint(0,maxhash-1)
    overlay = networkx.DiGraph()
    overlay.add_nodes_from(nodes)
    for n in nodes:
        myhash = hashids[n]
        #setup fingers for each node
        for i in range(0,hash_space):
            ideal_finger = (myhash + 2**i) % maxhash
            finger = min(nodes,key=lambda x: hash_dist(ideal_finger,hashids[x]))
            print finger,hashids[finger]
            overlay.add_edge(n,finger)
    return overlay, hashids

def create_chord_graph(nodes):
    hashids = {}
    for n in nodes:
        hashids[n] = random.randint(0,maxhash-1)
    overlay = networkx.DiGraph()
    overlay.add_nodes_from(nodes)
    for n in nodes:
        myhash = hashids[n]
        #setup fingers for each node
        for i in range(0,hash_space):
            ideal_finger = (myhash + 2**i) % maxhash
            finger = min(nodes,key=lambda x: hash_dist(ideal_finger,hashids[x]))
            print finger,hashids[finger]
            overlay.add_edge(n,finger)
    return overlay, hashids

def get_real_hops(real_graph,overlay,A,B):
    path = networkx.shortest_path(overlay,A,B)
    steps = []
    total = 0
    for i in range(1,len(path)):
        steps.append((path[i-1],path[i]))
    for s in steps:
        total = total + underlay.hops(real_graph,s[0],s[1])
    return total




#networkx.draw_circular(chord_overlay)
#plt.show()

if __name__ == "__main__":
    real_graph = underlay.generate_underlay(2000)
    with open("underlay_trail.csv","w+") as fp:
        writer = csv.writer(fp)
        for n in [100,500,1000,2000]:
            hoplist = []
            print "starting to generate overlay topology", n
            chord_overlay, hashids = create_chord_graph(random.sample(real_graph.nodes(),n))
            print "done generating topology: now sampling"
            for i in range(0,1000):
                x = random.choice(chord_overlay.nodes())
                y = random.choice(chord_overlay.nodes())
                while(x==y):
                    x = random.choice(chord_overlay.nodes())
                    y = random.choice(chord_overlay.nodes())

                hoplist.append(get_real_hops(real_graph,chord_overlay,x,y))
            mean = np.mean(hoplist)
            std = np.std(hoplist)
            writer.writerow([n,mean,std])
"""
    plt.hist(hoplist,bins=range(1,21))
    plt.title("Latency Distribution")
    plt.xlabel("Hops")
    plt.ylabel("Frequency")
    plt.show()
"""


