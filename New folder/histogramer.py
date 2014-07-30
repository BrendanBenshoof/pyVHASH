import csv
import matplotlib.pyplot as plt
import numpy as np
d=3

def get_raw(filename):
    results = {}
    with open(filename, 'rb') as csvfile:
        datareader =csv.reader(csvfile, delimiter=',')
        for row in datareader:
            method = row[1]
            size = row[0]
            samples = map(int,row[2:])
            if size not in results.keys():
                results[size] = {}
            results[size][method] = samples
            print size,method,samples[0]
    return results


#plt.xkcd()
results = get_raw("CHORD_VHASH_HIST_RAW.csv")
print results.keys()
for s in results.keys():
    plt.cla()
    labels = []
    data = []
    binmin = 1
    binmax = 1
    for m in results[s].keys():
        raw = results[s][m]
        labels.append(m)
        data.append(raw)
        tmpmax = max(raw)
        #tmpmin = min(raw)
        if tmpmax > binmax:
            binmax = tmpmax
    print len(data)

    plt.hist(data,normed=1, bins = range(1,binmax+1),label=results[s].keys(),color=["white","black"])
    plt.xlabel('Underlay Hops')
    plt.ylabel('Frequency')
    plt.title('Latency distribution with Overlay Network size: ' + str(s))
    plt.legend()
    plt.show()
