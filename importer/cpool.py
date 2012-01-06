import sys, os, time, datetime, Queue, thread
from threading import *
from couchdbkit import Server, Database
from restkit import Manager
import json
    

def worker(iworker, tout, uri, dbname, inQueue):
    pool = Manager(timeout=tout)
    s = Server(uri,pool_instance=pool)
    db = s[dbname]
    print 'worker:\t%i started for db %s' % (iworker, dbname)
    sys.stdout.flush()
    
    while (1):
        if (inQueue.empty()==True):
            time.sleep(0.0001)
            continue

        doc = inQueue.get()
        # print 'worker:\t%i\tsave doc:\t%s' % (iworker, doc['_id'])
        # sys.stdout.flush()
        # db.save_doc(doc)
        # db.save_doc(doc,batch='ok')

        #bulk
        db.save_docs(doc,batch='ok')
        # db.save_docs(doc)
        inQueue.task_done()
        
        sys.stdout.flush()

class CloudantPool(object):
    def __init__(self, nworkers, maxdepth, tsleep, timeout, uri, dbname):
        self.tstart = time.time()
        self.nworkers = nworkers
        self.maxdepth=maxdepth
        self.tsleep= tsleep
        self.timeout = timeout
        self.uri=uri
        self.dbname=dbname
        self.counter = 0
        
        #make sure DB exists
        pool = Manager(timeout=self.timeout)
        self.server = Server(self.uri,pool_instance=pool)
        self.db = self.server.get_or_create_db(self.dbname)
        print self.db.info()
        
        #create worker pool
        self.inputQ = Queue.Queue()
        self.workers = list()

        print '\nUse:\t%i workers' % self.nworkers
        for i in range(0,self.nworkers):
            #each worker gets its own connection
            thd = thread.start_new_thread(worker, (i,self.timeout, self.uri, self.dbname, self.inputQ) )
            self.workers.append(thd)
            
    def pushDoc(self, doc):
        #put it on the queue
        self.inputQ.put(doc)
        self.counter += 1
        if self.counter%100==0:
            print 'pushing doc:%i with depth %i' % (self.counter, self.inputQ.qsize())
            
        #keep the work queue from getting too backed up
        while (self.inputQ.qsize()>self.maxdepth):
            print 'qdepth = at %i  sleep(%f)' % (self.inputQ.qsize(), self.tsleep)
            sys.stdout.flush()
            time.sleep(self.tsleep)
            
    def flush(self):
        print '\nFinal Queue Flush'
        print 'size:\t', self.inputQ.qsize()
        sys.stdout.flush()
        self.inputQ.join()
        print 'done flushing'
        self.tstop = time.time()
        self.rate = float(self.counter)/float(self.tstop-self.tstart)
        print 'Saved %i documents in %i seconds for %f docs/sec' % (self.counter, self.tstop-self.tstart, self.rate)
        
    def cleanup(self):
        for thd in enumerate():
            print thd
#
if __name__=='__main__':
    
    #example usage
    pool = CloudantPool(10, 1000, 0.5, 1., 'http://uwser:pwd@user.cloudant.com', 'dbname')
    for doc in docs:
        pool.pushDoc(doc)
    
    #final flush
    pool.flush()

