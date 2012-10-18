import gevent.monkey; gevent.monkey.patch_all()

import socket
import urllib2
try:
    import json
except ImportError:
    import simplejson as json

from gevent_zeromq import zmq
import gevent
import gevent.monkey
import gevent.server
import gevent.socket
import gevent.queue
import gevent.event
import gevent.coros
import httplib2

from task import Task
import util
import constants

class TaskFailure(Exception): pass

class DispatchClient(object):
    def __init__(self, interface, callback):
        self.interface = interface
        self.callback = callback
        self.socket = None
    
    def start(self):
        gevent.spawn(self._run)
    
    def dispatch(self, task):
        if self.socket:
            self.socket.send('%s\n' % task.serialize())
    
    def _run(self):
        while True:
            try:
                self.socket = gevent.socket.create_connection((self.interface, 6002), source_address=(self.interface, 0))
            
                for line in util.line_protocol(self.socket):
                    event, payload = line.split(':', 1)
                    self.callback(event, payload)
            except IOError:
                pass
                    
            print "disconnected from dispatcher, retrying..."
        

class Dispatcher(object):
    def __init__(self, interface, zmq_context, workers=10):
        # hardcoding for now
        self.workers = workers
        self.server = gevent.server.StreamServer((interface, 6002), self._connection_handler)
        self.queue = gevent.queue.Queue()
        self.scheduler = None
        self.zmq = zmq_context
        self.zmq_sockets = {}
    
    def start(self, block=True):
        self.server.start()
        for n in xrange(self.workers):
            gevent.spawn(self._dispatcher)
        
        while block:
            gevent.sleep(1)
    
    def _dispatcher(self):
        http = httplib2.Http()
        while True:
            try:
                task = Task.unserialize(self.queue.get())
                timeout = gevent.Timeout(constants.WORKER_TIMEOUT)
                timeout.start()
                self.scheduler.send('start:%s\n' % task.id)
                
                if task.url.startswith('http'):
                    headers = {"User-Agent": "Miyamoto/0.1", "X-Task": task.id, "X-Queue": task.queue_name, "Content-type": "application/json"}
                    resp, content = http.request(task.url, method=task.method, headers=headers, body=task.params)
                    if task.result_callback_url and content:
                        result_content_type = resp.get_header('Content-type')
                        result_data = content
                        if util.walks_like_json(result_content_type):
                            result_data = json.loads(content)
                        elif util.talks_like_form(result_content_type):
                            result_data = dict([(k,v[0]) for k,v in urlparse.parse_qs(content).items() ])
                        else:
                            try:
                                result_data = json.loads(content)
                            except Exception, e:
                                try:
                                    result_data = dict([(k,v[0]) for k,v in urlparse.parse_qs(content).items() ])
                                except Exception, e:
                                    pass
                        result_data.update({'status': resp.status, 'reason': resp.reason, 'headers': resp.getheaders()})
                        callback_resp, callback_content = http.request(task.result_callback_url, method='POST', body=json.dumps(result_data))
                else:
                    zmq_remotes = frozenset(task.url.split(','))
                    if not zmq_remotes in self.zmq_sockets:
                        sock = self.zmq.socket(zmq.REQ)
                        lock = gevent.coros.Semaphore()
                        for remote in zmq_remotes:
                            sock.connect(remote)
                        self.zmq_sockets[zmq_remotes] = (sock, lock)
                    else:
                        sock, lock = self.zmq_sockets[zmq_remotes]
                    try:
                        lock.acquire() # Because send/recv have to be done together
                        sock.send(task.url)
                        resp = sock.recv()
                    except zmq.ZMQError:
                        raise
                    finally:
                        lock.release()
                self.scheduler.send('success:%s\n' % task.id)
            except (gevent.Timeout, zmq.ZMQError, TaskFailure), e:
                self.scheduler.send('failure:%s:%s\n' % (task.id, str(e)))
            finally:
                timeout.cancel()
    
    def _connection_handler(self, socket, address):
        print "pair connected"
        self.scheduler = socket
        for line in util.line_protocol(socket):
            self.queue.put(line)
        print "pair dropped"
    
