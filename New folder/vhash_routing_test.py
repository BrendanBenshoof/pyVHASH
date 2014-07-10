from vhash_greedy import *
import random

random.seed(12345)

def simulate_routing(nodes):
    pass


class Node(object):
    def __init__(self):
        self.loc = randPoint()
        self.peers =  []
        
    # works only because this is a  static simulation
    def gossip(self, peers):
        yenta = random.choice(peers) # yenta is yiddish for a woman unable to keep a secret
        
        my_candidates = self.peers + list(set(yenta.peers) - set(self.peers))  # In set notaion A <- A union B
        yenta_candidates = yenta.peers + list(set(self.peers) - set(yenta.peers))
        
        self.update_peers(my_candidates)
        yenta.update_peers(yenta_candidates)

        
    def update_peers(self,candidates):
        pass
        
    def approx_region(self, candidates):
        new_peers = []
        candidates =  sorted(candidates, key = lambda x: dist(self.loc, x.loc)) #sort candidates
        new_peers =  candidates.pop(0)
        for c in candidates[:]:
            midpoint = calc_midpoint(self.loc,c)
            


# Goals print out routing success rate, average degree, largest degree

if __name__ ==  '__main__':
    print "okay"
