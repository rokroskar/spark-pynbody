#!/bin/env python
import subprocess
import re
import matplotlib.pylab as plt
from  pickle import dump
import sys
import numpy as np
import sqlite3
import matplotlib.patches as mpatches

class bc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

times = {'lustre': [], 'hdfs': []}

regex = 'took\s(\d+\.\d+)\sseconds,\s(\d+\.\d+)'
size =  220150628352

templates = {'lustre': 'bsub -W 00:10 -n {ncores} -R span[ptile=4] -I /cluster/apps/spark/scripts/lsf-spark-submit.sh --executor-memory 3G ./read_sim.py lustre {ncores}',
             'hdfs': '/cluster/home03/sdid/roskarr/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --num-executors {num_executors} --executor-cores 4 --master yarn ./read_sim.py hdfs {ncores}'}

def run_io_benchmark(fs_types, executors): 

    for fs_type in fs_types: 
        for num_executors in executors: 
        
            process = subprocess.Popen(templates[fs_type].format(ncores=num_executors*4, num_executors=num_executors).split(' '), 
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            while True: 
                nextline = process.stdout.readline()
                if nextline == '' and process.poll() != None: 
                    break

                sys.stdout.write(nextline)
                sys.stdout.flush()

                time_match = re.findall(regex, nextline)
                UI_match = re.findall('SparkUI\sat\s(.+)', nextline)

                if len(time_match) > 0: 
                    print bc.OKGREEN+'running on %d executors '%num_executors + 'time elapsed %.2f, %.2f Gb/s'%tuple(map(lambda x: float(x), time_match[0]))+bc.ENDC
                    times[fs_type].append(float(time_match[0][0]))
                
                if len(UI_match) > 0: 
                    print bc.HEADER+'Spark UI at: '+ str(UI_match[0])+bc.ENDC
            
            with open('times_dict','w') as f:
                dump(times, f)
        

def get_db_set(c): 
    return set(get_db_list(c))

def get_db_list(c):
    return map(lambda x: x[0], c.fetchall())

def plot_data(db_name):
    from glob import glob

    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    filesystems = get_db_set(c.execute("select filesystem from io_timing"))
    executor_dict = {fs: get_db_set(c.execute("select num_executors from io_timing where filesystem = '%s'"%fs)) for fs in filesystems} 

    data = {fs: {num_executors: np.array(get_db_list(c.execute("select elapsed_time from io_timing where filesystem = '{fs}' and num_executors = {num_executors}".format(fs=fs, num_executors=num_executors)))) for num_executors in executor_dict[fs]} for fs in filesystems}

    plt.style.use('fivethirtyeight')
    fig, axs = plt.subplots()

    color_cycler = axs._get_lines.prop_cycler
    legend_handles = []
    for fs in filesystems: 
        color = next(color_cycler)['color']
        legend_handles.append(mpatches.Circle((0,0), 0.5, color=color, label=fs))
        for nx, points in data[fs].iteritems():
            plt.scatter(np.array([nx]*len(points)),size/points/1e9, color = color)
            
    plt.legend(handles=legend_handles)
    plt.xlabel('Number of nodes')
    plt.ylabel('Gb/s')

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run a series of I/O tests")

    parser.add_argument('executors', type=int, nargs='+', help='executor counts to run I/O on')

    parser.add_argument('--fs_types', type=str, nargs='+', dest='fs_types', default='hdfs')

    args = parser.parse_args()
    
    executors = args.executors
    fs_types = args.fs_types

    run_io_benchmark(fs_types, executors)


