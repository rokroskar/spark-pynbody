#!/bin/env python

import pynbody
import os
import pyspark
import numpy as np
from time import time, sleep
import sqlite3

os.environ['SPARK_CONF_DIR'] = os.path.realpath('./spark_config')

s = pynbody.load('/cluster/home03/sdid/roskarr/work/testing/cosmo25cmb.768g2_dm.001024')
size = 220150628352

# here we set the memory we want spark to use for the driver JVM
os.environ['SPARK_DRIVER_MEMORY'] = '2G' #'%dG'%(ncores*2*0.7)

g_dt = s._g_dtype
d_dt = s._d_dtype

sc = pyspark.SparkContext(appName="read_sim")

def write_to_db(db_name, filesystem, ncores, time, elapsed_time):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    c.execute("INSERT into io_timing VALUES(?, ?, ?, ?, ?, ?)", (filesystem, ncores/4, ncores, time, elapsed_time, size))
    conn.commit()
    conn.close()

if __name__ == "__main__": 
    import argparse

    parser = argparse.ArgumentParser(description='Run an I/O benchmark using the specified filesystem')

    parser.add_argument('filesystem', action='store', help='which filesystem to use; hdfs or lustre')

    parser.add_argument('ncores', action='store', help='number of cores to be used', type=int)

    args = parser.parse_args()

    filesystem = args.filesystem
    ncores = args.ncores

    if filesystem == 'lustre': 
        filename = 'file:///cluster/scratch_xp/shareholder/sis/hol/cosmo25cmb.768g2_dm.001024_dm_36Mb_blocks'
    elif filesystem == 'hdfs':
        filename = '/user/roskarr/nbody/cosmo25cmb.768g2_dm.001024_dm_36Mb_blocks'

    while sc.defaultParallelism != ncores:
        sleep(2)
        print 'Need %d, have %d cores'%(ncores, sc.defaultParallelism)

    dm_rdd = sc.binaryRecords(filename,
                              d_dt.itemsize*1024)\
                              .map(lambda x: np.fromstring(x, dtype=d_dt).byteswap())\
                              .flatMap(lambda arr: [arr[i] for i in xrange(len(arr))]) 
                              

    time_in = time()
    dm_rdd.count()
    time_tot = time()-time_in
    write_to_db('io_timing.db', filesystem, ncores, time_in, time_tot)
    print 'processing took %.2f seconds, %.2f Gb/s'%(time_tot, size/float(time_tot)/1e9)
    sc.stop()

    






