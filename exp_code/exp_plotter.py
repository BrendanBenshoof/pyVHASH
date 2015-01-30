from matplotlib import pyplot as plt

xs = []
ys = []

with open("exp_log.txt","r") as fp:
    for l in fp:
        l = l.strip()
        a,b = map(int,l.split(' '))
        xs.append(a)
        ys.append(b)

plt.plot(xs,ys,'o')
plt.show()