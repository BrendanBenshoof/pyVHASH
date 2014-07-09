#simulates vhash network to test hit-rate
from vol_util import *
from vhash_greedy import *
max_update_size = 10

def hit_miss_test(nodes):
        correct = 0.0
        samples = 1000
        for i in range(0,samples):
                p = randPoint()
                start = random.choice(nodes)
                end = start.seek(p)
                right = min(nodes,key=lambda x: dist(x.loc,p))
                if end == right:
                        correct+=1.0
        return correct/float(samples)



class Node(object):
        def __init__(self):
                self.loc = randPoint()
                self.short_peers = []
                self.long_peers = []

        def get_notified(self, canidates):
                for c in canidates:
                        if c != self:
                                if c not in self.long_peers:
                                        self.long_peers.append(c)

        def join(self,newguy):
                parent = self.seek(newguy.loc)
                newguy.get_notified(parent.short_peers+[parent])
                newguy.notify_peers()
                newguy.update_peerlist()
                parent.update_peerlist()
                parent.notify_peers()


        def seek(self,loc):
                mydist = dist(self.loc,loc)
                if len(self.short_peers) == 0:
                        return self
                else:
                        best_peer = min(self.short_peers+self.long_peers,key=lambda x: dist(x.loc,loc))
                        if dist(loc, best_peer.loc) < mydist:
                                return best_peer.seek(loc)
                        else:
                                return self

        def probe(self,loc):
                mydist = dist(self.loc,loc)
                if len(self.short_peers) == 0:
                        return 0
                else:
                        best_peer = min(self.short_peers+self.long_peers,key=lambda x: dist(x.loc,loc))
                        if dist(loc, best_peer.loc) < mydist:
                                return 1+best_peer.probe(loc)
                        else:
                                return 0
        def notify_peers(self):
                for p in self.short_peers:
                        bundle = self.short_peers[:]
                        bundle.append(self)
                        for p2 in sorted(self.long_peers,key=lambda x: dist(x.loc,p.loc)):
                                if len(bundle) >= max_update_size:
                                        break
                                if p2 not in bundle:
                                        bundle.append(p2)
                        p.get_notified(bundle)

        def update_peerlist(self):
                if len(self.long_peers) == 0:
                        return
                locs = {}
                for p in self.long_peers:
                        locs[p.loc] = p
                peers = getShell(self.loc,locs.keys())
                self.short_peers = []
                for p in peers:
                        self.short_peers.append(locs[p])

all_nodes = [Node()]
# Inserts n nodes into the network
n =100
generations = 100
for i in range(0,n):
        new_node = Node()
        contact_node = random.choice(all_nodes)
        contact_node.join(new_node)
        all_nodes.append(new_node)

for i in range(0,generations):
        print hit_miss_test(all_nodes)
        print map(lambda x: len(x.short_peers), all_nodes)
        print map(lambda x: len(x.long_peers), all_nodes)
        for n in all_nodes:
                n.notify_peers()
                n.update_peerlist()
