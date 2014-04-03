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

    def distribute(self,pile_of_data, origin):
        pile_of_data = load(pile_of_data)
        forwards = {}
        mystuff = []
        for p in pile_of_data:
            ploc = Name2loc(assciiafy(p))
            key = self.getBestForward(ploc)
            if key is None:
                mystuff.append(p)
            elif key in forwards:
                forwards[key].append(p)
            else:
                forwards[key] = [p]
        for k in forwards:

            Peer(k).distribute(dump(forwards[k]),origin)
        results = map(self.map,mystuff)
        single_result = reduce(self.reduce,results)
        Peer(origin).collate(single_result)

    def collate(single_result):
        self.data.append(single_result)

    def setup(self,filename):
        lines = []
        with open(filename,"r") as fp:
            map(lambda x: lines.append(convert(x)),fp)
        self.distribute(dump(lines),self.name)

    def report(self):
        final = reduce(self.reduce,self.data)
        return final
