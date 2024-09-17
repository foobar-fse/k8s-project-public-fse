#!/usr/bin/env python

import os
import json
import re
from optparse import OptionParser
from utility_3 import *

'''
polling mount info with 'mount' in the node only once
Note: we will infer the persistent disk(PD) in 172.16.112.85:/mnt/nfs_k8s_pv from the mount info
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/11/24'


def mount_poll(outpath):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath)
    
    # list mount info with 'mount' (sudo user)  
    f1 = os.popen("mount") #
    lines = f1.read().splitlines()
    
    records = list()
    keys = ['fs_spec', 'fs_file', 'fs_vsftype', 'fs_mntopts']
    for line in lines:
        # 'sysfs on /sys type sysfs (rw,nosuid,nodev,noexec,relatime)'
        vals = re.split(' on | type | \(|\)', line)[:-1] # ignore the last '' after split
        record = dict(list(zip(keys, vals)))
        #print record
        records.append(record)

    # write the docker-images brief info
    with open('%s/mount_out.json' % subpath, 'w') as fo:
        print(json.dumps(records, sort_keys=True, indent=4), file=fo) # without indent can save space

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    mount_poll(options.outpath)

if __name__ == '__main__':
    main()

