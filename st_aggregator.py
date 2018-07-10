import os
import redis
import gdal
from gdal import osr, ogr
import numpy as np
from rwcsv import ServeCSV
import Geohash


class STAggregator:

    def __init__(self, spatial_agg_dir, APpath, startmonth, endmonth, tempstep, stoutfields):
        self.APpath = APpath
        self.spatial_agg_dir = spatial_agg_dir
        self.startmonth = startmonth
        self.endmonth = endmonth
        self.tempstep = tempstep
        self.outfields = stoutfields
        self.persistent_detections = []
        self.new_detections = []
        self.monthlist = self.list_months()
        self.csvdict = self.compile_csvdict()
        self.csvserver = ServeCSV(self.csvdict, spatial_agg_dir, APpath)
        self.redis_up()       
        self.iter_months() 
    


    def setup(self, code):
        self.persistent = Persistent(code, self.csvserver, self.r)
        self.appearant = Appearant(code, self.csvserver, self.r, self.persistent)
        self.pkey = self.persistent.get_key()
        self.akey = self.appearant.get_key()
        self.key = code

    def sortkeys(self, keys):
	def sortval(a):
	    return int(str(a)[:-2])

	keys.sort(key=lambda x: x[-1])
	yrlist = sorted(elem[-1] for elem in keys)
	yrset = set(yrlist)
	yrlist = sorted(yrset)
	keys = map(int, keys)
	for yr in yrlist:
	    x = sorted((w for w in keys if str(w)[-1] == yr), key=sortval)
	    y = iter(x) 
	    keys = [w if str(w)[-1] != yr else next(y) for w in keys]
	keys = map(str, keys)
	return keys

	
    def iter_months(self):
	keys = self.csvdict.keys()
        sortedkeys = self.sortkeys(keys)
        for currentcode in sortedkeys:
	    print "Working on Month with code: ", currentcode
            self.setup(currentcode)
            detections = self.pull_csv(currentcode)
            self.detectiondict = self.build_ids(detections)
            self.index_builder(currentcode, self.detectiondict)
            self.search()        


    #get the values from a given csv using the key for that filepath
    def pull_csv(self, code):
        detectiondict = self.csvserver.read_csv_cut(code)
	return detectiondict

    #compile a key-value dict of codes and csv paths
    def compile_csvdict(self):
        dictobj = {}
        for elem in self.monthlist:
            code = str(elem[0]) + str(elem[1])[2:]
            csvpath = os.path.join(self.spatial_agg_dir, '{}.csv'.format(code))
            dictobj[code] = csvpath
	
        return dictobj

    def list_months(self):
        monthlist = []
        month1 = self.startmonth
        while True:
	    code = str(month1[0]) + str(month1[1])[2:]
            if os.path.exists(os.path.join(self.spatial_agg_dir, '{}.csv'.format(code))):
                monthlist.append(month1)
            if month1 == self.endmonth:
                break
            else:
                grow_month = self.month_can_grow(month1)
                if grow_month:
                    month1 = [month1[0] + 1, month1[1]]
                else:
                    month1 = [1, month1[1] + 1]
        return monthlist


    def month_can_grow(self, date):
        month = date[0]
        if month == 12:
            return False
        else:
            return True

    #Build geohash ids for each dict location
    def build_ids(self, ddict):
        for elem in ddict:
            x = elem['c_x']
            y = elem['c_y']
            elem['geohash'] = self.latlon2hash(y, x)
        return ddict

    def latlon2hash(self, lat, lon):
        g = Geohash.encode(float(lat), float(lon), precision=11)
	return g

    def hash2latlon(self, geohash):
        g = Geohash.decode(geohash)
        return [float(g[0]), float(g[1])]


    #TODO
    #build a redis index with a key and dictionary of values (can be ori or agg)
    def index_builder(self, key, detectiondict):
        self.r.flushall()
        """self.pipe.reset()
        for elem in detectiondict:
	    
            self.pipe.geoadd(key, elem['c_x'], elem['c_y'], elem['geohash'])

        self.pipe.execute()
        self.pipe.reset()"""

    
    def redis_up(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.pipe = self.r.pipeline()

    #shuts down a redis instance
    def redis_down(self):
        #self.r.shutdown()
        return

    def search(self):
        self.neighbour_search_backwards(self.key, self.akey, self.pkey)
        self.appearant.post_search_process()
        self.persistent.post_search_process()
        self.appearant.shutdown_sequence()
        self.persistent.shutdown_sequence()


    def neighbour_search_backwards(self, key, akey, pkey):
        for elem in self.detectiondict:
            ghash = elem['geohash']
            lat, lon = self.hash2latlon(ghash)
            phits = self.r.georadius(pkey, lon, lat, 100, unit='m')
            if len(phits) > 0:
                #update persistent
                self.persistent.update_collection_found(phits)
                continue
            ahits = self.r.georadius(akey, lon, lat, 1000, unit='m')
            if len(ahits) > 0:
		print ahits
                self.appearant.update_collection_found(ahits)
                continue
            if (len(phits) == 0) and (len(ahits) == 0):
                self.appearant.queue_add_hash(ghash)



#holds the detectiondict of persistent points for given timecode and responsible for manipulations
#if elements of Persistent satisfy certain conditions, they are added to a seperate category of points that are static with information about appearance date, confidence, etc
class Persistent:

    def __init__(self, code, csvserver, redisInstance):
	pullcode = csvserver.get_earlier_code(code)
        self.detectiondict = csvserver.read_APcsv(pullcode, 'P')
        self.csvserver = csvserver
        self.code = code
        self.r = redisInstance
        self.toremove = []
        self.key = self.code + 'P'
        self.addqueue = []
        self.init_redis()
        self.init_redis_db()



    def init_redis(self):
        self.pipe = self.r.pipeline()


    def init_redis_db(self):
        for elem in self.detectiondict:
	    lat, lon = self.hash2latlon(elem['geohash'])
            self.pipe.geoadd(self.key, lon, lat, elem['geohash'])

        self.pipe.execute()
        self.pipe.reset()

    def write_detections(self):
        self.csvserver.write_APcsv(self.code, 'P', self.detectiondict)

    def queue_add_hash(self, elem):
        self.addqueue.append(elem)

    def add_hashes(self):
        for elem in self.addqueue:
            self.add_hash(elem)


    def add_hash(self, nelem):
        for elem in self.detectiondict:
            if elem.get('geohash') == nelem['geohash']:
                return
        self.detectiondict.append(nelem)

    def queue_rm_hash(self, geohash):
        self.toremove.append(geohash)

    def remove_hashes(self):
        for geohash in self.toremove:
            self.detectiondict[:] = [d for d in self.detectiondict if d.get('geohash') == geohash]

    def latlon2hash(self, lat, lon):
        g = Geohash.encode(float(lat), float(lon), precision=11)
	return g

    def hash2latlon(self, geohash):
        g = Geohash.decode(geohash)
        return [float(g[0]), float(g[1])]

    def get_points(self):
        return self.detectiondict


    def get_key(self):
        return self.key

    #foreach elem in persistent, update n, N, ls, and fs
    #if N
    #TODO is there a better way to handle this then iterating a dict for each fucking detection?  Answer is yes: do it
    #TODO Maybe dict instead of list?
    def update_collection_found(self, aggregation):
        for elem in self.detectiondict:
            if elem['geohash'] in aggregation:
                if int(elem['lastspotted']) > 0:
                    elem['lastspotted'] = 0
                    N = int(elem['N'])
                    n = int(elem['n'])
                    elem['N'] = N + 1
                    elem['n'] = n + 1
                    break

    def shutdown_sequence(self):
        self.r.delete(self.key)
        self.write_detections()

    def post_search_process(self):
        for elem in self.detectiondict:
            if elem['lastspotted'] > 0:
                N = int(elem.get('N'))
                elem['N'] = str(N + 1)
            ls = int(elem['lastspotted'])
            elem['lastspotted'] = ls + 1
        self.remove_hashes() #may not be necessary
        self.add_hashes()




#holds the detectiondict of points that appeared in the last comparison.  If they appear twice in a row, they will be moved to the classification of 'persistent'.
class Appearant:
    
    def __init__(self, code, csvserver, redisInstance, p):
	pullcode = csvserver.get_earlier_code(code)
	print 'pullA', pullcode
        self.detectiondict = csvserver.read_APcsv(pullcode, 'A')
	self.csvserver = csvserver
        self.code = code
        self.r = redisInstance
        self.to_remove = []
        self.key = self.code + 'A'
        self.init_redis()
        self.init_redis_db()
        self.addqueue = []
        self.persistent = p
        self.topersistent = []



    def init_redis(self):
        self.pipe = self.r.pipeline()


    def init_redis_db(self):
	print self.key
        for elem in self.detectiondict:
            lat, lon = self.hash2latlon(elem['geohash'])
            self.pipe.geoadd(self.key, lom, lat, elem['geohash'])
	self.pipe.execute()
        self.pipe.reset()
	

    def write_detections(self):
        self.csvserver.write_APcsv(self.code, 'A', self.detectiondict)

    def add_hash(self, geohash, N, n, firstspotted, lastspotted):
        self.detectiondict.append({'geohash': geohash, 'N': N, 'n': n, 'firstspotted': firstspotted, 'lastspotted': lastspotted})

    def queue_add_hash(self, geohash):
        self.addqueue.append(geohash)

    def add_hashes(self):
        for geohash in self.addqueue:
            self.add_hash(geohash, 1, 1, self.code, 1)

    def queue_rm_hash(self, geohash):
        self.to_remove.append(geohash)

    def remove_hashes(self):
        for geohash in self.to_remove:
            self.detectiondict[:] = [d for d in self.detectiondict if d.get('geohash') == geohash]

    def latlon2hash(self, lat, lon):
        g = Geohash.encode(float(lat), float(lon), precision=11)
	return g

    def hash2latlon(self, geohash):
        g = Geohash.decode(geohash)
        return [float(g[0]), float(g[1])]


    def get_points(self):
        return self.detectiondict


    def get_key(self):
        return self.key

    def move_topersistent(self):
        for elem in self.topersistent:
            self.persistent.queue_add_hash(elem)
            self.queue_rm_hash(elem['geohash'])


    #TODO refer to this method implemented in Persistent
    def update_collection_found(self, aggregation):
        for elem in self.detectiondict:
            if elem['geohash'] in aggregation:
                if int(elem['lastspotted']) > 0:
                    elem['lastspotted'] = 0
                    N = int(elem['N'])
                    n = int(elem['n'])
                    elem['N'] = N + 1
                    elem['n'] = n + 1
                    break

    def shutdown_sequence(self):
        self.r.delete(self.key)
        self.write_detections()

    def post_search_process(self):
        for elem in self.detectiondict:
            if int(elem['lastspotted']) > 5:
		#print 'c1'
                self.to_remove.append(elem['geohash'])
            elif int(elem['lastspotted']) == 0 and int(elem['n']) >= 2:
                ls = int(elem['lastspotted'])
                elem['lastspotted'] = ls + 1
                self.topersistent.append(elem)
		#1print 'c2'
            else:
                ls = int(elem['lastspotted'])
                elem['lastspotted'] = ls + 1
		N = int(elem['N'])
                elem['N'] = N + 1
		#print 'c3'

        self.move_topersistent()
        self.remove_hashes()
        self.add_hashes()


"""
#holds the detectiondict for the current class
class Current(SPT_Collection):

    def __init__(self, code, csvserver, redisInstance):
        super(Current, self).__init__(code, csvserver, redisInstance)
        self.key = self.code + 'C'

    def init_redis(self):
        super(Current, self).init_redis()

    def init_redis_db(self):
        super(Current, self).init_redis_db()

    def add_hash(self, geohash, x, y, N, n, lastspotted):
        super(Current, self).add_hash(geohash, x, y, N, n, lastspotted)

    def queue_rm_hash(self, geohash):
        super(Current, self).queue_rm_hash(geohash)

    def remove_hashes(self):
        super(Current, self).remove_hashes()

    def latlon2hash(self, lat, lon):
        super(Current, self).latlon2hash(lat, lon)

    def hash2latlon(self, geohash):
        super(Current, self).hash2latlon(geohash)

    def get_points(self):
        super(Current, self).get_points()

    def get_key(self):
        super(Current, self).get_key()



class SPT_Collection(object):

    def __init__(self, code, csvserver, redisInstance):
        self.detectiondict = csvserver.read_csv_withhash(code)
        self.csvserver = csvserver
        self.code = code
        self.r = redisInstance
        self.to_remove = []
        self.init_redis()
        self.init_redis_db()


    def init_redis(self):
        self.pip e = self.r.pipeline()
        self.r.delete(self.code)

    def init_redis_db(self):
        for elem in self.detectiondict:
            self.pipe.geoadd(key, elem['c_x'], elem['_y'], elem['geohash'])

        self.pipe.execute()
        self.pipe.reset()

    def add_hash(self, geohash, x, y, N, n, lastspotted):
        self.detectiondict.append({'geohash': geohash, 'x': x, 'y': y, 'N': N, 'n': n, 'lastspotted': lastspotted})


    def queue_rm_hash(self, geohash):
        self.toremove.append(geohash)

    def remove_hashes(self):
        for geohash in self.toremove:
            self.detectiondict[:] = [d for d in self.detectiondict if d.get('geohash') == geohash]

    def latlon2hash(self, lat, lon):
        return Geohash.encode(lat, lon, precision=11)

    def hash2latlon(self, geohash):
        g = Geohash.decode(geohash)
        return [float(g[0]), float(g[1])]


    def get_points(self):
        return self.detectiondict

    def get_key(self):
        return self.key

"""
