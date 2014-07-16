from vhash_greedy import *
import matplotlib.pyplot as plt
import networkx as nx
import random
from math import log

G=nx.DiGraph()
random.seed(12345)
setd(2)



NETWORK_SIZE = 5000
n = float(NETWORK_SIZE)
TABLE_SIZE = 2*int(log(n)/log(log(n)))+getd()
print TABLE_SIZE

CYCLES = 100
CHATTY_JOIN = False
USING_LONG_PEERS = True

def simulate_routing(nodes):
    correct = 0.0
    samples = 2000
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
    def gossip(self, yenta =  None):
        if yenta is None:
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
        if USING_LONG_PEERS:
            for c in candidates:
                if c not in self.long_peers and c is not self:
                    self.long_peers.append(c)
            #if len(self.long_peers) > TABLE_SIZE*TABLE_SIZE:
            #    self.long_peers = random.sample(self.long_peers,TABLE_SIZE*TABLE_SIZE )
            
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
        if not False:
            while len(new_peers) < TABLE_SIZE and len(candidates) > 0:  #is this block nessecary
                new_peers.append(candidates.pop(0))
        return new_peers

    def lookup(self, loc):
        if len(self.peers) == 0:
            return self
        best_peer = None
        if USING_LONG_PEERS:
            best_peer =  min(self.peers+self.long_peers, key = lambda x: dist(loc, x.loc))
        else:
            best_peer =  min(self.peers, key = lambda x: dist(loc, x.loc))
        mydist = dist(loc, self.loc)
        if dist(loc, best_peer.loc) < mydist:
            return best_peer.lookup(loc)
        else:
            return self
            
    def join(self, member):  # sorta the reverse of how it was previously done
        parent = member.lookup(self.loc)
        if CHATTY_JOIN:
            self.peers = [parent] + parent.peers
            for p in self.peers[:]:
                self.gossip(p)
        else:
            self.peers = [parent]
            self.gossip()

# Goals print out routing success rate, average degree, largest degree

if __name__ ==  '__main__':
    nodes = []
    bootstrapper = Node()
    nodes.append(bootstrapper) 
    for i in range(1,NETWORK_SIZE):
        n = Node()
        """
        parent = random.choice(nodes)
        n.join(parent)
        
        """
        nodes.append(n)
        """
        if i%50 ==0:
            for node in nodes:
                node.gossip()
            print simulate_routing(nodes), i
        """
    print "DONE ADDING"

    accuracy_list = []
    for i in range(0,CYCLES):
        centerist = 0# max(nodes, key= lambda x: len(x.peers))
        saddest =  NETWORK_SIZE#min(nodes, key= lambda x: len(x.peers))
        centerist_long = 0
        saddest_long = NETWORK_SIZE
        total_short = 0.0
        total_long = 0.0
        for node in nodes:
            if i < 2:
                node.peers.extend(random.sample(nodes,10))

            node.gossip()
            tmp = len(node.peers)
            tmp_long = len(node.long_peers)
            if tmp > centerist:
                centerist=tmp
            if tmp_long > centerist_long:
                centerist_long=tmp_long
            if tmp < saddest:
                saddest = tmp
            if tmp_long < saddest_long:
                saddest_long = tmp_long
            total_short += tmp
            total_long += tmp_long


        accuracy = simulate_routing(nodes)
        print i, accuracy, saddest, total_short/float(NETWORK_SIZE), centerist, saddest_long, total_long/float(NETWORK_SIZE), centerist_long
        accuracy_list.append(accuracy)
    plt.plot(range(0,100), accuracy_list)
    plt.show()
        #print [len(x.peers)  for x in sorted(nodes, key= lambda x: len(x.peers)) ]
        #print saddest.loc, [x.loc for x in saddest.peers]
        #print centerist.loc, [x.loc for x in centerist.peers]