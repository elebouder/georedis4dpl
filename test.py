import redis
import os
from rwcsv import ServeCSV
import numpy as np
import Geohash

csvpath = '/media/elebouder/Seagate Backup Plus Drive/aggregations/georedis4dpl/intra_agg/415.csv'
code = '415'

ds = ServeCSV({code: csvpath}, '/media/elebouder/Seagate Backup Plus Drive/aggregations/georedis4dpl/intra_agg', '/media/elebouder/Seagate Backup Plus Drive/aggregations/georedis4dpl/inter_agg')
ddict = ds.read_csv_cut(code)
adict = ds.read_APcsv('315', 'A')
r = redis.Redis(host='localhost', port=6379, db=0)

def hash2latlon(geohash):
    g = Geohash.decode(geohash)
    return [float(g[0]), float(g[1])]

for elem in adict:
    lat, lon = hash2latlon(elem['geohash'])
    r.geoadd('315A', lon, lat, elem['geohash'])

for elem in ddict:
    ahits = r.georadius('315A', elem['c_x'], elem['c_y'], 100, unit='m')
    print ahits




