import os
import numpy as np
from s_aggregator import SAggregator 
from st_aggregator import STAggregator

raw_data_dir = '/media/elebouder/Seagate Backup Plus Drive/detection_csv'
agg_base = '/media/elebouder/Seagate Backup Plus Drive/aggregations'
spatial_agg_dir = agg_base + '/georedis4dpl/intra_agg'
spatiotemp_agg_dir = agg_base + '/georedis4dpl/inter_agg'
temp_outfields = ['geohash', 'N', 'n', 'firstspotted', 'lastspotted']
#will there be inter-month processing?  If true, it assumes that all months that will be processed as such have already been aggregated.
temporal = True
#start and end months for spatiotemporal processing.  Will process in step-month timesteps
temp_start = [4, 2015]
temp_end = [4, 2015]
step = 1
#start and end months for spatial processing.  Each month is processed individually
spat_start = [1, 2017]
spat_end = [12, 2017]
spat_outfields = ['c_x', 'c_y']

#will initialize an aggregator class that pulls detections from csv, computes ids, pushes them
#into month-based redis keys, performs for-each radial member search, and for each aggregate calculates the centroid coordinate and pushes it all to a new key representing the aggregated month


if not temporal:
    aggregator = SAggregator(raw_data_dir, spatial_agg_dir, spat_start, spat_end, spat_outfields)
elif temporal:
    aggregator = STAggregator(spatial_agg_dir, spatiotemp_agg_dir, temp_start, temp_end, step, temp_outfields)
