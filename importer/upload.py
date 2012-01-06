from couchdbkit import Server, Database
from cpool import CloudantPool
import random, string, time
from copy import deepcopy

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))
    
if __name__=='__main__':
    #CHANGE THESE TWO LINES TO SPECIFY CLUSTER AND CREDS
    uri = 'http://<username>:<pwd>@<cluster/user>.cloudant.com'
    dbname = 'rate_test'
    
    #NUMBER OF DOCUMENTS TO UPLOAD
    ndocs = 100000
    nworkers = 40 #n_threads
    
    s = Server(uri)
    db = s.get_or_create_db(dbname)
    print db.info()
    
    #create writer pool
    pool = CloudantPool(nworkers, 100, 0.1, 1., uri, dbname)
    
    start = time.time()
    original = start
    step = 1000
    ndocs = 100000
    docs = []
    
    for i in range(0,ndocs):
        if (i%step)==0:
            delta = float(time.time()-start)
            rate = float(step)/delta
            print 'saved:\t%i\tdocs in:\t%f\tseconds for:\t%f\tdocs/sec' % (step, delta, rate)
            start = time.time()
        email = '%s@%s.com' % (id_generator(10), id_generator(5, chars=string.ascii_lowercase))
        pwd = id_generator(40)
        doc = {'email':email, 'pwd':pwd}
        
        docs.append(doc)
        if len(docs)==200:
            pool.pushDoc(deepcopy(docs))
            del docs
            docs = []
        
        
    pool.flush()
    
    delta = float(time.time()-original)
    rate = float(ndocs)/delta
    print 'Summary Statistics\n\nsaved:\t%i\tdocs in:\t%f\tseconds for:\t%f\tdocs/sec' % (ndocs, delta, rate)
    