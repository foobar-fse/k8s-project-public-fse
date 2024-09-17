#!/usr/bin/env python

import os
import re
import json
from optparse import OptionParser
from utility import *

'''
polling the cpu+mem with 'top -b -n 1' in the node only once,
and parse the 'top' output in json format
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/12/07'


def cpu_mem_poll(outpath):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath)
    
    # get cpu and mem usage with 'top' command 
    f1 = os.popen("top -b -n 1") # take one snapshot with -n1
    blocks = f1.read().split('\n\n') # summary and processes are separated by a blank line
        
    res = dict()
    # parse the summary info
    summary = blocks[0].splitlines()
    # 'top - 10:45:50 up 11 days, 1 min,  1 user,  load average: 9.41, 10.29, 9.50'
    res['top'] = summary[0] # not further extract
    # Tasks: 1095 total,   1 running, 648 sleeping,   0 stopped,   0 zombie
    task_key = ['total', 'running', 'sleeping', 'stopped', 'zombie']
    task_val = re.findall(r'\b\d+\b', summary[1]) # extract integer
    res['task'] = dict(zip(task_key, task_val))
    # '%Cpu(s):  8.6 us,  2.6 sy,  0.0 ni, 86.2 id,  1.3 wa,  0.0 hi,  1.3 si,  0.0 st'
    cpu_key = ['us', 'sy', 'ni', 'id', 'wa', 'hi', 'si', 'st'] 
    cpu_val = re.findall(r'\b\d+\.?\d+\b', summary[2]) # extract float
    res['cpu'] = dict(zip(cpu_key, cpu_val))
    # 'KiB Mem : 19668584+total, 10727251+free, 44646432 used, 44766892 buff/cache'
    ram_key = ['total', 'free', 'used', 'buff/cache']
    ram_val = re.findall(r'\b\d+', summary[3]) # extract integer, ignore the '+', the last char may not be space
    res['ram'] = dict(zip(ram_key, ram_val)) 
    # 'KiB Swap:        0 total,        0 free,        0 used. 15112976+avail Mem '
    swap_key = ['total', 'free', 'used', 'avail']
    swap_val = re.findall(r'\b\d+', summary[3]) # extract integer, ignore the '+', the last char may not be space
    res['swap'] = dict(zip(swap_key, swap_val))

    # parse the process info
    lines = blocks[1].splitlines()
    proc_key = re.split('\s+', lines[0].strip())
    procs = list()
    for line in lines[1:]:
        proc_val = re.split('\s+', line) # not need strip
        proc = dict(zip(proc_key, proc_val))
        procs.append(proc)
    res['process'] = procs

    # write the cpu+ram metric info
    with open('%s/cpu_ram_metric_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(res, sort_keys=True, indent=4) # without indent can save space

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    cpu_mem_poll(options.outpath)

if __name__ == '__main__':
    main()

