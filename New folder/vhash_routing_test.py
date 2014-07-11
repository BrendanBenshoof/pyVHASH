from vhash_greedy import *
import random

random.seed(12345)
TABLE_SIZE = 3*d +1
NETWORK_SIZE = 1000
CYCLES = 1000

def simulate_routing(nodes):
    correct = 0.0
    samples = 1000
    for i in range(0,samples):
        p = randPoint()
        start = random.choice(nodes)
        end = start.lookup(p)
        right = min(nodes,key=lambda x: dist(x.loc,p))
        if end == right:
                correct+=1.0
    return correct/float(samples)



class Node(object):
    def __init__(self):
        self.loc = randPoint()
        self.peers =  []
        
    # works only because this is a  static simulation
    def gossip(self):
        yenta = random.choice(self.peers) # yenta is yiddish for a rumormonger
        
        #need to remove self and yenta from our own lists
        my_candidates = self.peers + list(set(yenta.peers) - set(self.peers))  # In set notaion A <- A union B
        yenta_candidates = yenta.peers + list(set(self.peers) - set(yenta.peers))
        
        self.update_peers(my_candidates)
        yenta.update_peers(yenta_candidates)

        
    def update_peers(self,candidates):
        self.peers = self.approx_region()
        
    def approx_region(self, candidates):
        new_peers = []
        candidates =  sorted(candidates, key = lambda x: dist(self.loc, x.loc)) #sort candidates
        new_peers.append(candidates.pop(0))
        for c in candidates[:]:
            midpoint = calc_midpoint(self.loc,c)  # find the midpoint between myself and the new point
            dist_to_midpoint = dist(self.loc, midpoint)
            good = True
            for p in new_peers:  # now for each peer that I've added
                if dist(p.loc, midpoint) < dist_to_midpoint: # if p is closer to the midpoint than I am...
                    good = False
                    break # reject it
            if good:
                candidates.remove(c)
                new_peers.append(c)
        while len(new_peers) < TABLE_SIZE and len(candidates) > 0:
            new_peers.append(candidates.pop(0))
        return new_peers

    def lookup(self, loc):
        if len(self.peers) == 0:
            return self
        best_peer =  min(self.peers, key = lambda x: dist(loc, x.loc))
        mydist = dist(loc, self.loc)
        if dist(loc, best_peer.loc) < mydist:
            return best_peer.seek(loc)
        else:
            return self
            
    def join(self, member):  # sorta the reverse of how it was previously done
        parent = member.lookup(self.loc)
        self.peers.append(parent)
        self.gossip()

# Goals print out routing success rate, average degree, largest degree

if __name__ ==  '__main__':
    nodes = []
    for i in range(0,NETWORK_SIZE):
        n = Node()
