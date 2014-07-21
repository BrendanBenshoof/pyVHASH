import csv
import matplotlib.pyplot as plt
import numpy as np
d=5

def get_accuracy(filename):
    accuracy = []
    with open(filename, 'rb') as csvfile:
        datareader =csv.reader(csvfile, delimiter=',')
        for row in datareader:
            accuracy.append(row[1])
    return accuracy


plt.xkcd()
plt.plot(np.arange(0,100),get_accuracy('n_500_d_'+str(d)+'.csv'), label='n=500', marker='o', linestyle= '-', color='b')
plt.plot(np.arange(0,100),get_accuracy('n_1000_d_'+str(d)+'.csv'), label='n=1000', marker='.', linestyle= '-', color='r')
plt.plot(np.arange(0,100),get_accuracy('n_2000_d_'+str(d)+'.csv'), label='n=2000', marker='*', linestyle= '-', color='g')
plt.plot(np.arange(0,100),get_accuracy('n_5000_d_'+str(d)+'.csv'), label='n=5000', marker='x', linestyle= '-', color='k')
#plt.plot(np.arange(0,100),get_accuracy('n_10000_d_'+str(d)+'.csv'), label='n=10000', marker='p', linestyle= '-', color='y')

plt.xlabel('Cycle')
plt.ylabel('Accuracy')
plt.title('Routing Convergence in d=' +str(d))
plt.legend()
plt.show()