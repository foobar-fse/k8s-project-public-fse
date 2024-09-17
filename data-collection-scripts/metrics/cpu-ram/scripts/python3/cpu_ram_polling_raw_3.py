#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility_3 import *

'''
polling the cpu+mem with 'top -b -n 1' in the node only once
keep the shell output without parsing 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/11/05'


def cpu_mem_poll(outpath):
    #mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath)
    subpath2 = "\ ".join(subpath.split()) # shell command requires path without space.    
    
    # take one snapshot of 'top' command (-n 1)
    os.system('top -b -n 1  > %s/cpu_ram_metric_out.txt' % subpath2)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    cpu_mem_poll(options.outpath)

if __name__ == '__main__':
    main()

