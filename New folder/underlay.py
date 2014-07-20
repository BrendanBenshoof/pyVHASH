import numpy as np
import csv
import networkx, random
import matplotlib.pyplot as plt
import datetime
#keeps track of an underlay network
import timeit

def generate_underlay(size):
    return networkx.scale_free_graph(size, seed=12345).to_undirected()

def hops(G,A,B):
    #number of hops from A to B on G
    return networkx.shortest_path_length(G,A,B)

if __name__ == "__main__":
    start = timeit.default_timer()
    underlay = generate_underlay(2000)
    with open("underlay_perfect_trial.csv","w+") as fp:
        writer = csv.writer(fp)
        for n in [100,500,1000,2000]:
            hoplist = []
            subset = random.sample(underlay.nodes(),n)
            for i in range(0,1000):
                x = random.choice(subset)
                y = random.choice(subset)
                while(x==y):
                    x = random.choice(subset)
                    y = random.choice(underlay.nodes())

                hoplist.append(hops(underlay,x,y))
            mean = np.mean(hoplist)
            std = np.std(hoplist)
            writer.writerow([n,mean,std])


"""
    plt.hist(hoplist,bins=range(1,21))
    plt.title("Latency Distribution")
    plt.xlabel("Hops")
    plt.ylabel("Frequency")
    stop = timeit.default_timer()
    print stop - start 
    plt.show()
"""

