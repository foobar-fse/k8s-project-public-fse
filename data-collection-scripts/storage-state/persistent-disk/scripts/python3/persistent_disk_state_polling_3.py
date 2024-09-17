#!/usr/bin/env python

import os
import re
import json
from optparse import OptionParser
from utility_3 import *

'''
polling the persistent disk (PD) state in NFS, such as existense, size, ctime
the PD corresponds to a directory in NFS, we list the depth=1 dirs
NOTE: the size is not the total size of subdirs. (too slow to sum-up with du) 
we focus on the existense and read-write mode 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/09/14'


def PD_state_poll(inpath, outpath):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have remove space and colon in utility.py
    
    # list persistent disks in NFS as a folder
    keys = ['mode', 'user', 'group', 'size','ctime', 'name']
    printformat = '%m\t%u\t%g\t%s\t%CY-%Cm-%Cd %.12CT %CZ\t%p\n' # mode, user, group, size, ctime, name
    f1 = os.popen("sudo find %(inpath)s  -maxdepth 1  -mindepth 1  -type d  -printf '%(printformat)s'"\
    	% {'inpath': inpath, 'printformat': printformat})	
    lines = f1.read().splitlines()
    persistent_disks = list()
    for line in lines:
    	vals = line.split('\t') 	
    	pd = dict(list(zip(keys, vals)))
    	#print pd
    	persistent_disks.append(pd)
    
    # write the docker-images brief info
    with open('%s/persistent_disks_out.json' % subpath, 'w') as fo:
       	print(json.dumps(persistent_disks, sort_keys=True, indent=4), file=fo) # without indent

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-i', '--inpath', metavar='inpath', default='/mnt/k8s_nfs_pv', help='NFS path for persistent disks')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    PD_state_poll(options.inpath, options.outpath)

if __name__ == '__main__':
    main()

