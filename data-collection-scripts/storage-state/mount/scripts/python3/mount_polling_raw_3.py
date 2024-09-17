#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility_3 import *

'''
polling mount info with 'mount' in the node only once
keep the shell output without parsing 
Note: we will infer the persistent disk(PD) in 172.16.112.85:/mnt/nfs_k8s_pv from the mount info
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/11/24'


def mount_poll(outpath):
    #mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have removed space and colon in utiltiy.py

    # list mount info with 'mount' (sudo user) 
    os.system('mount > %s/mount_out.txt' % subpath)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    mount_poll(options.outpath)

if __name__ == '__main__':
    main()

