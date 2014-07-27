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
                results = {}
                results[size] = {}
            results[size][method] = samples
    print results['100'].keys()
    return results


#plt.xkcd()
results = get_raw("CHORD_VHASH_HIST_RAW.csv")
for s in results.keys():
    plt.cla()
    labels = []
    data = []
    for m in results[s].keys():
        raw = results[s][m]
        labels.append(m)
        data.append(raw)
    print len(data)
    plt.hist(data,normed=1, label=labels,color=["red","blue"])
    plt.xlabel('Underlay Hops')
    plt.ylabel('Frequency')
    plt.title('Overlay Network size' + str(s))
    plt.legend()
    plt.show()
