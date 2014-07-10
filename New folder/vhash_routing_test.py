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
        tmp = self.peers
        self.peers = self.peers + list(set(yenta.peers) - set(self.peers))  # In set notaion A <- A union B
        print tmp, self.peers
        yenta.peers = yenta.peers + list(set(tmp) - set(yenta.peers))
        self.update_region()
        yenta.update_region()
        
    def update_region(self):
        pass
        
        


# Goals print out routing success rate, average degree, largest degree
        
