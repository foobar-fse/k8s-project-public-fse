#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility import *

'''
polling local disk usage  with 'df -h' in the node only once 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/11/05'


def disk_metric_poll(outpath):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath)
    
    # list disk usage with 'df -h'  
    f1 = os.popen("df -h") #
    lines = f1.read().splitlines()
    
    records = list()
    keys = ['Filesystem', 'Size', 'Used', 'Avail', 'Use%', 'Mounted on']
    for line in lines[1:]:
        vals = line.split()
        record = dict(zip(keys, vals))
        #print record
        records.append(record)

    # write the disk metric info
    with open('%s/disk_metric_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(records, sort_keys=True, indent=4)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    disk_metric_poll(options.outpath)

if __name__ == '__main__':
    main()

