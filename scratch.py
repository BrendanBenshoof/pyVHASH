from threading import Thread


class Test(object):
    
    
    def __init__(self,friend):
        self.friend = friend
        self.isCounting = True
        self.num = 0
    
    def startFriend(self):
        t = Thread(friend.countUp)
        t.daemon = True
        t.start()
   
        
    def stopFriend
        
    def countUp(self):
        while isCounting:
            self.num = self.num+1
        
a = Test()

a.f("c")
print a.myVar
