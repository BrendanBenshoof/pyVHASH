from os import path
from pyChord import getHashString, getHash
import json


DEFAULT_BLOCK_SIZE = 1024*8 # bytes
MAX_BLOCK_SIZE = float("inf")


class DataAtom(object):
    def __init__(self,contents,hashid=None):
        #assumes contents are objects not jsonDUMPs
        if hashid is None:
            self.hashid = getHashString(str(contents))  # do we want a long or string?  Assuming string 
        else:
           self.hashid = hashid
        self.contents = contents

    def __str__(self):
        return str(self.contents)



class KeyFile(object):
    def __init__(self, filename):
        self.hashid = getHashString(filename)
        self.keys = [] #list of identfiers

    def __str__(self):
        return str(self.keys)

    def __iter__(self):
        return keys

    def foo(x,y):
        return x+y
                
    """
    @classmethod
    def parse(cls,string):
        summary = unpack(string)
        k = cls()
        k.name = summary["name"]
        k.chunklist = map(hash_util.Key, summary["chunklist"])
        return k
    """





def convert(i):#de-unicodes json
    if isinstance(i, dict):
        return {convert(key): convert(value) for key, value in i.iteritems()}
    elif isinstance(i, list):
        return [convert(element) for element in i]
    elif isinstance(i, unicode):
        return i.encode('utf-8')
    else:
        return i

def pack(stuff):
    return json.dumps(stuff,separators=(',',':'))

def unpack(string):
    return convert(json.loads(string))

###UTILITY FUNCTIONS###

def asciifilter(text):
    return ''.join([i if ord(i) < 128 else '' for i in text])

def lineChunk(fname):
    with open(fname, 'rb') as fin:
        return list(iter(fin.readline,''))

def wordChunk(fname):
    raw_text = ""
    with open(fname, 'rb') as fin:
        raw_text = fin.read()
    return raw_text.split()

def binaryChunk(fname):
    with open(fname, 'rb') as fin:
        return list(iter(lambda: asciifilter(fin.read(DEFAULT_BLOCK_SIZE)), ''))

def binaryChunkPack(chunkIterator,maxsize=DEFAULT_BLOCK_SIZE):
    #assumes you are using strings
    #if a single chunk is bigger than maxsize, it sends it anyway
    current_chunk = ""
    for c in chunkIterator:
        if len(current_chunk) + len(c)+1 <= maxsize:
            current_chunk+=" "+c
        else:
            yield current_chunk
            current_chunk = c
    if current_chunk:#empty string is falsey
        yield current_chunk

def logicalChunk(chunkIterator):
    return map(lambda c: DataAtom(c,None), chunkIterator)

def locgicalBinaryChunk(filename):
    return logicalChunk(binaryChunk(filename))

###END UTILITY FUNCTIONS###

# For the purposes of laziness and sanity
# Each block should be treated as an individual file
def makeBlocks(filename, chunkgen=locgicalBinaryChunk):
    kf = KeyFile(filename)
    blocks = []
    for block in chunkgen(filename):
        kf.keys.append(block.hashid)
        blocks.append(block)
    return (kf, blocks)

