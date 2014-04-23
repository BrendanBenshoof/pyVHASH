from SimpleXMLRPCServer import SimpleXMLRPCServer

class Server(object):
    def __init__(self, hostport):
        self.server = SimpleXMLRPCServer(hostport)

    def register_function(self, function, name=None):
        def _function(args, kwargs):
            return function(*args, **kwargs)
        _function.__name__ = function.__name__
        self.server.register_function(_function, name)

    def serve_forever(self):
        self.server.serve_forever()

#example usage

class ServerProxy(object):
    def __init__(self, url):
        self._xmlrpc_server_proxy = xmlrpclib.ServerProxy(url)
    def __getattr__(self, name):
        call_proxy = getattr(self._xmlrpc_server_proxy, name)
        def _call(*args, **kwargs):
            return call_proxy(args, kwargs)
        return _call
#example usage


server = Server(('localhost', 9000))
def test(arg1, arg2):
    print 'arg1: %s arg2: %s' % (arg1, arg2)
    return 0
server.register_function(test)
server.serve_forever()



serverp = ServerProxy('http://localhost:9000')
serverp.test(1, 2)
serverp.test(arg2=2, arg1=1)
serverp.test(1, arg2=2)
serverp.test(*[1,2])
serverp.test(**{'arg1':1, 'arg2':2})