from threading import Thread
import time

class Test(object):
    
    
    def __init__(self,friend = None):
        self.friend = friend
        self.isCounting = False
        self.num = 0
    
    def startFriend(self):
        t = Thread(target = self.friend.countUp)
        t.daemon = True
        t.start()
   
        
    def stopFriend(self):
        self.friend.isCounting =False 
        
    def countUp(self):
        self.isCounting = True
        while self.isCounting:
            self.num = self.num+1
    
    def getCount(self):
        return self.num
a = Test()
b = Test(a)
a.friend = b
a.startFriend()
print b.getCount()
a.stopFriend()
print b.getCount()
time.sleep(0.5)
print b.getCount()

print b.getCount()
