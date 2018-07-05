f hit not in local_cluster_proposalsimport redis
import os
from rwcsv import ServeCSV
import numpy as np


csvpath = '/home/elebouder/3_2014.csv'
code = 314

ds = ServeCSV({code: csvpath})
ddict = ds.read_csv(code)

r = redis.Redis(host='localhost', port=6379, db=0)
r.set('foo', 'bar')
print(r.get('foo'))


def build_ids(ddict):
    for elem in ddict:
        scene = elem['SID']
        conf = elem['confidence']
        idbase = str(int(abs(np.random.uniform() * 1000000)))
        geoid = scene +'_' + conf + '_' + idbase
        elem['geoid'] = geoid
        

    return ddict

iddict = build_ids(ddict)
pipe = r.pipeline()
for elem in iddict:
    
    pipe.geoadd(code, elem['c_x'], elem['c_y'], elem['geoid'])

a = pipe.execute()

for elem in iddict:
    pipe.geohash(code, elem['geoid'])

for elem in iddict:
   if r.geopos(code, elem['geoid']):
       geoid = elem['geoid']

       hit1 = r.georadiusbymember(code, geoid, 1, unit='km')
      


r.flushall()  
