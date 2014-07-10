from vhash_greedy import *
import random

random.seed(12345)
TABLE_SIZE = 3*d +1

def simulate_routing(nodes):
    pass


class Node(object):
    def __init__(self):
        self.loc = randPoint()
        self.peers =  []
        
    # works only because this is a  static simulation
    def gossip(self, peers):
        yenta = random.choice(peers) # yenta is yiddish for a rumormonger
        
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
        


            


# Goals print out routing success rate, average degree, largest degree

if __name__ ==  '__main__':
    print "okay"
