from pyVhash import Node, Peer, Name2locString, string2loc, Name2loc
import json
import time

def dump(input):
    return convert(json.dumps(input))

def load(input):
    return json.loads(input)


def convert(input):
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

def assciiafy(string):
    output = ""
    for l in string:
        if ord(l) < 128:
            output+=l
    return output

class MapReduceNode(Node):
    def __init__(self,ip,port):
        Node.__init__(self,ip,port)
        self.data = {}
        self.addnewFunc(self.distribute,"distribute")
        self.addnewFunc(self.collate,"collate")
        self.data = []

    def map(self, line):
        #example problem is wordcount
        freq = {}
        for w in line.split():
            if w in freq.keys():
                freq[w]+=1
            else:
                freq[w]=1
        return freq

    def reduce(self, freq1, freq2):
        for k in freq2:
            if k in freq1.keys():
                freq1[k]+=freq2[k]
            else:
                freq1[k]=freq2[k]
        return freq1

    def distribute(self, dict_of_data, origin):
        forwards = {}
        mystuff = {}
        print dict_of_data
        for l in dict_of_data:
            mybest = self.getBestForward(string2loc(l))
            print mybest
            if mybest is None:
                mystuff[l] = dict_of_data[l]
            elif mybest in forwards:
                forwards[mybest] = {l:dict_of_data[l]}
            else:
                forwards[mybest][l] = dict_of_data[l]
                print forwards, mybest, l
        for k in forwards:
            Peer(k).distribute(forwards[k],origin)
        results = map(lambda x: self.map(mystuff[x]) , mystuff.keys())
        print "mystuff",mystuff
        time.sleep(1)
        single_result = reduce(self.reduce,results)
        Peer(origin).collate(single_result)

    def collate(single_result):
        self.data.append(single_result)

    def setup(self,filename):
        lines = []
        data = {}
        with open(filename,"r") as fp:
            for l in fp:
                l_loc = Name2locString(assciiafy(l))
                data[l_loc]=l
        self.distribute(data,self.name)

    def report(self):
        final = reduce(self.reduce,self.data)
        return final
