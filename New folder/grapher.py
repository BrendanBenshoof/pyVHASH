import csv
import matplotlib.pyplot as plt
import numpy as np
d=3

def get_accuracy(filename):
    accuracy = []
    with open(filename, 'rb') as csvfile:
        datareader =csv.reader(csvfile, delimiter=',')
        for row in datareader:
            accuracy.append(row[1])
    return accuracy


#plt.xkcd()
plt.plot(np.arange(0,100),get_accuracy('n_500_d_'+str(d)+'.csv'), label='Network Size = 500', marker='o', linestyle= '-.')
plt.plot(np.arange(0,100),get_accuracy('n_1000_d_'+str(d)+'.csv'), label='Network Size = 1000', marker='.', linestyle= '-')
plt.plot(np.arange(0,100),get_accuracy('n_2000_d_'+str(d)+'.csv'), label='Network Size = 2000', marker='*', linestyle= '--')
plt.plot(np.arange(0,100),get_accuracy('n_5000_d_'+str(d)+'.csv'), label='Network Size = 5000', marker='x', linestyle= '-', color= 'm')
plt.plot(np.arange(0,100),get_accuracy('n_10000_d_'+str(d)+'.csv'), label='Network Size = 10000', marker='p', linestyle= ':', color='k')

plt.axis([0,40.0,0,1.0])
plt.xlabel('Cycles')
plt.ylabel('Accuracy')
plt.title('Dimension = ' + str(d))
plt.legend()
plt.show()