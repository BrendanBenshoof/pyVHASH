from vhash_greedy import *
import matplotlib.pyplot as plt
import networkx as nx
import random


G=nx.DiGraph()
random.seed(12345)
TABLE_SIZE = 3*d +1
NETWORK_SIZE = 100
CYCLES = 100

def simulate_routing(nodes):
    correct = 0.0
    samples = 2
    for i in range(0,samples):
        p = randPoint()
        start = random.choice(nodes)
        end = start.lookup(p)
        right = min(nodes,key=lambda x: dist(x.loc,p))
        if end == right:
                correct+=1.0
    return correct/float(samples)

def get_avg_degree(nodes):
    total = 0.0
    for n in nodes:
        total = total + len(n.peers)
    return total/float(len(nodes))



class Node(object):
    def __init__(self):
        self.loc = randPoint()
        self.peers =  []
        self.long_peers = []
        
    # works only because this is a  static simulation
    def gossip(self):
        yenta = random.choice(self.peers) # yenta is yiddish for a rumormonger
        
        #need to remove self and yenta from our own lists
        my_candidates = self.create_candidates(yenta)  # In set notaion A <- A union B
        yenta_candidates = yenta.create_candidates(self)
        
        self.update_peers(my_candidates)
        yenta.update_peers(yenta_candidates)

    def create_candidates(self, yenta):
        candidates = self.peers
        rumors = [yenta] + yenta.peers 
        for r in rumors:
            if r not in candidates and not(r is self):  # possibe error point
                candidates.append(r)
        return candidates

        
    def update_peers(self,candidates):
        self.peers = self.approx_region(candidates)
        for c in candidates:
            if c not in self.long_peers and c is not self:
                self.long_peers.append(c)
        if len(self.long_peers) > TABLE_SIZE*TABLE_SIZE:
            self.long_peers = random.sample(self.long_peers,TABLE_SIZE*TABLE_SIZE )
        
    def approx_region(self, candidates):
        new_peers = []
        candidates =  sorted(candidates, key=lambda x: dist(self.loc, x.loc)) #sort candidates
        new_peers.append(candidates.pop(0))
        for c in candidates[:]:
            midpoint = calc_midpoint(self.loc,c.loc)  # find the midpoint between myself and the new point
            dist_to_midpoint = dist(self.loc, midpoint)
            good = True
            for p in new_peers:  # now for each peer that I've added
                if dist(p.loc, midpoint) < dist_to_midpoint: # if p is closer to the midpoint than I am...
                    good = False
                    break # reject it
            if good:
                candidates.remove(c)
                new_peers.append(c)
        #while len(new_peers) < TABLE_SIZE and len(candidates) > 0:  #is this block nessecary
        #    new_peers.append(candidates.pop(0))
        return new_peers

    def lookup(self, loc):
        if len(self.peers) == 0:
            return self
        best_peer =  min(self.peers+self.long_peers, key = lambda x: dist(loc, x.loc))
        mydist = dist(loc, self.loc)
        if dist(loc, best_peer.loc) < mydist:
            return best_peer.lookup(loc)
        else:
            return self
            
    def join(self, member):  # sorta the reverse of how it was previously done
        parent = member.lookup(self.loc)
        self.peers.append(parent)
        self.gossip()
        #or gossip with all peers

# Goals print out routing success rate, average degree, largest degree

if __name__ ==  '__main__':
    nodes = []
    bootstrapper = Node()
    nodes.append(bootstrapper) 
    for i in range(1,NETWORK_SIZE):
        n = Node()
        parent = random.choice(nodes)
        n.join(parent)
        nodes.append(n)
        """
        if i%50 ==0:
            for node in nodes:
                node.gossip()
            print simulate_routing(nodes), i
        """

    print "DONE ADDING"


    for i in range(0,CYCLES):
        for node in nodes:
            node.gossip()
        print i, simulate_routing(nodes), get_avg_degree(nodes), len(max(nodes, key= lambda x: len(x.peers)).peers)


    G.add_nodes_from(nodes)
    for n in nodes:
        for p in n.peers:
            G.add_edge(n,p)

    print G.number_of_nodes()
    print G.number_of_edges()


    nx.draw(G)
    plt.show()
    """
    for node in nodes:
        print node.loc, [x.loc for x in node.peers]
    """