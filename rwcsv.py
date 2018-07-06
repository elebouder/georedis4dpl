import csv
import os
import Geohash

class ServeCSV:
    def __init__(self, csvdict, path=None, APpath=None):
        self.spatial_csvdict = csvdict
	self.APpath = APpath
        self.path = path


    def read_spatial_csv(self, code):
        csvpath = self.spatial_csvdict[code]
        detectiondict = []
        with open(csvpath, 'r') as cf:
            reader = csv.DictReader(cf)
            for row in reader:
                if not self.is_num(row['xmin']):
                    continue
                xmin = row['xmin']
                xmax = row['xmax']
                ymin = row['ymin']
                ymax = row['ymax']
                cx = row['c_x']
                cy = row['c_y']
                confidence = row['confidence']
                sid = row['scene']
                dictelem = {'xmin': xmin, 'xmax': xmax, 'ymin': ymin, 'ymax': ymax, 'c_x': cx, 'c_y': cy, 'confidence': confidence, 'SID': sid}
                detectiondict.append(dictelem)

        return detectiondict


    def is_num(self, string):
        try:
            float(string)
            return True
        except ValueError:
            return False


    def write_Sagg_csv(self, code, outdir, aggpoints, fields):
        path = outdir + '/' + code + '.csv'
        with open(path, 'w') as cf:
	    writer = csv.DictWriter(cf, fields)
            writer.writeheader()
            for row in aggpoints:
		writer.writerow({'c_x': row[0], 'c_y': row[1]})		            


    def read_csv_cut(self, code):
        csvpath = self.spatial_csvdict[code]
        detectiondict = []
        with open(csvpath, 'r') as cf:
            reader = csv.DictReader(cf)
            for row in reader:
                if not self.is_num(row['c_x']):
                    continue 
                cx = row['c_x']
                cy = row['c_y']
                dictelem = {'c_x': cx, 'c_y': cy}
                detectiondict.append(dictelem)

        return detectiondict

    def read_APcsv(self, code, AorP):
        csvpath, exists = self.get_AP(code, self.APpath, AorP)
	if not exists:
	    return [{}]
        detectiondict = []
        with open(csvpath, 'r') as cf:
            reader = csv.DictReader(cf)
            for row in reader:
                geohash = row['geohash']
                
                N = row['N']
		n = row['n']
		firstspotted = row['firstspotted']
		lastspotted = row['lastspotted']
                dictelem = {'geohash': geohash, 'N': N, 'n': n, 'firstspotted': firstspotted, 'lastspotted': lastspotted}
                detectiondict.append(dictelem)

        return detectiondict

    def hash2latlon(self, geohash):
        g = Geohash.decode(geohash)
        return [float(g[0]), float(g[1])]


    def write_APcsv(self, namecode, AorP, data, fieldnames=['geohash', 'N', 'n', 'firstspotted', 'lastspotted', 'lat', 'lon']):
        csvout = os.path.join(self.APpath, '{}_{}.csv'.format(namecode, AorP))
        with open(csvout, 'w') as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for row in data:
		ghash = row['geohash']
		lat, lon = self.hash2latlon(ghash)
                writer.writerow({'geohash': row['geohash'], 'N': row['N'], 'n': row['n'], 'firstspotted': row['firstspotted'], 'lastspotted': row['lastspotted'], 'lat': lat, 'lon': lon})

    def get_earlier_code(self, code):
	if len(code) == 3:
	    month = code[0]
 	elif len(code) == 4:
            month = code[0:2]
        yr = code[-2:]
	if int(month) == 1:
	    month = '12'
            yr = str(int(yr) - 1)
            newcode = month + yr
            return newcode
	else:
	    month = str(int(month) - 1)
            newcode = month + yr
	    return newcode        	



    def get_AP(self, code, csvpath, AorP):
	path_str = csvpath + '/' + code + '_' + AorP + '.csv'
	if os.path.exists(path_str):
	    return path_str, True
	else:
	    return '', False	 
