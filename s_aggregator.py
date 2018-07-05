import os
import redis
import gdal
from gdal import osr, ogr
import numpy as np
from rwcsv import ServeCSV


class SAggregator:

    def __init__(self, raw_data_dir, spatial_agg_dir, startmonth, endmonth, outfields):
        self.raw_data_dir = raw_data_dir
        self.spatial_agg_dir = spatial_agg_dir
        self.startmonth = startmonth
        self.endmonth = endmonth
        self.outfields = outfields
        self.monthlist = self.list_months()
        self.csvdict = self.compile_csvdict()
        self.csvserver = ServeCSV(self.csvdict)
        self.redis_up()       
        self.iter_months() 
    
    def iter_months(self):
        
        for currentcode,path in self.csvdict.items():
            detections = self.pull_csv(currentcode)
            detectiondict = self.build_ids(detections)
            self.index_builder(currentcode, detectiondict)
            aggpoints = self.neighbour_search(currentcode, detectiondict)
            self.csvserver.write_Sagg_csv(currentcode, self.spatial_agg_dir, aggpoints, self.outfields)
        


    #get the values from a given csv using the key for that filepath
    def pull_csv(self, code):
        detectiondict = self.csvserver.read_csv_cut(code)
        return detectiondict

    #compile a key-value dict of codes and csv paths
    def compile_csvdict(self):
        dictobj = {}
        for elem in self.monthlist:
            code = str(elem[0]) + str(elem[1])[2:]
            csvpath = os.path.join(self.raw_data_dir, '{}_{}.csv'.format(elem[0], elem[1]))
            dictobj[code] = csvpath
        return dictobj

    def list_months(self):
        monthlist = []
        month1 = self.startmonth
        while True:
            if os.path.exists(os.path.join(self.raw_data_dir, '{}_{}.csv'.format(month1[0], month1[1]))):
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
  
    

    def build_ids(self, ddict):
        for elem in ddict:
            #scene = elem['SID']
            #scene = 'fffds'
            #conf = elem['confidence']
            idbase = str(int(abs(np.random.uniform() * 1000000000)))
            #geoid = scene +'_' + conf + '_' + idbase
            geoid = idbase
            elem['geoid'] = geoid
        return ddict



    #build a redis index with a key and dictionary of values (can be ori or agg)
    def index_builder(self, key, detectiondict):
        self.r.flushall()
        self.pipe.reset()
        for elem in detectiondict:
            self.pipe.geoadd(key, elem['c_x'], elem['c_y'], elem['geoid'])

        self.pipe.execute()
        self.pipe.reset()

    
    #initializes a redis instance belonging to this object
    def redis_up(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.pipe = self.r.pipeline()

    #shuts down a redis instance
    def redis_down(self):
        #self.r.shutdown()
        return

    #conduct a neighbour search in an index
    def neighbour_search(self, key, detectiondict):
        aggpoints = []
        for elem in detectiondict:
            print(self.r.geopos(key, elem['geoid']))
            if self.r.geopos(key, elem['geoid'])[0] is not None:
                localcluster = self.get_local_cluster(key, elem['geoid'])
                agg_geopoint = self.compute_aggregate(key, localcluster)
                self.kill_cluster(key, localcluster)
                aggpoints.append(agg_geopoint)
        return aggpoints


    #get the cluster of local geopoints that can be reached by stepping along minimum radial search
    def get_local_cluster(self, key, geoid):
        print(self.r.geopos(key, geoid))
        hit1 = self.r.georadiusbymember(key, geoid, 200, unit='m')
        local_cluster_proposals = hit1
        print("++++++++++++++++++++++")
        print(hit1)
        print("++++++++++++++++++++++")
        for elem in hit1:
            hit2 = self.r.georadiusbymember(key, elem, 200, unit='m')
            print("=======================")
            print(hit2)
            print("=======================")
            for hit in hit2:
                if hit not in local_cluster_proposals:
                    local_cluster_proposals.append(hit)
        return local_cluster_proposals

    #given a set of geopoints, compute weighted spatial averages
    #given the small search areas and the moderate latitude, naive averages will be relatively accurate, but for points closer to the poles or international dateline this must be replaced with spherical projection meausures or something
    def compute_aggregate(self, key, localcluster):
        count = len(localcluster)
        xsum = 0.00
        ysum = 0.00
        for elem in localcluster:
            geopos = self.r.geopos(key, elem)[0]
            
            xsum += geopos[0]
            ysum += geopos[1]
            
        xmean = xsum/count
        ymean = ysum/count
        return [xmean, ymean]
        

    #remove from the spatial map the original local cluster of geopoints from which the mean point was created
    def kill_cluster(self, key, localcluster):
        for elem in localcluster:
            self.r.zrem(key, elem)
