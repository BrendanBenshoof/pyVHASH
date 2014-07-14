import networkx, underlay
import math, random
import matplotlib.pyplot as plt
import vhash_greedy as vhash


def generate_vhash_graph(nodes):
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
    for n in nodes:
        far_peers[n] = []
    for n in nodes:
        for p in close_peers[n]:
            for p2 in close_peers[n]:
                if p2 not in far_peers[p] and p2 != p:
                    far_peers[p].append(p2)
    for n in nodes:
        for p in close_peers[n]:
            overlay.add_edge(n,p)
        for p in far_peers[n]:
            overlay.add_edge(n,p)
    return overlay

def generate_optimized_vhash_graph(nodes,real,gens):
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
        for n in nodes:
            far_peers[n] = []
        for n in nodes:
            for p in close_peers[n]:
                for p2 in close_peers[n]:
                    if p2 not in far_peers[p] and p2 != p:
                        far_peers[p].append(p2)
        for n in nodes:
            weighted_vectors = []
            weight_sum = 0.0
            for p in close_peers[n]:
                latency = float(underlay.hops(real,n,p))
                delta_vec = map(lambda x,y: min([y-x,vhash.space_size-(y-x)])/latency, locs[p], locs[n])
                weight_sum+=latency**-1.0
                weighted_vectors.append(delta_vec)
            delta_vec = map(lambda x: x/weight_sum, reduce(vhash.vec_sum, weighted_vectors))
            del revlocs[locs[n]]
            locs[n] = tuple(vhash.vec_sum(locs[n],delta_vec))
            revlocs[locs[n]] = n
    for n in nodes:
        for p in close_peers[n]:
            overlay.add_edge(n,p)
        for p in far_peers[n]:
            overlay.add_edge(n,p)
    return overlay

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
    hoplist = []
    real_graph = underlay.generate_underlay(1000)
    chord_overlay = generate_optimized_vhash_graph(random.sample(real_graph.nodes(),100), real_graph, 20)

    for i in range(0,100):
        x = random.choice(chord_overlay.nodes())
        y = random.choice(chord_overlay.nodes())
        while(x==y):
            x = random.choice(chord_overlay.nodes())
            y = random.choice(chord_overlay.nodes())

        hoplist.append(get_real_hops(real_graph,chord_overlay,x,y))

    plt.hist(hoplist,bins=range(1,21))
    plt.title("Latency Distribution")
    plt.xlabel("Hops")
    plt.ylabel("Frequency")
    plt.show()