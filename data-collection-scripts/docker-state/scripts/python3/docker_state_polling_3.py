#!/usr/bin/env python

import os
import re
import json
from optparse import OptionParser
from utility_3 import *

'''
polling the container's physical state in Docker runtime in the node, run only once.
container state keeps involving, therefore, we snapshot every period, instead of comparing the difference of consective states. 
# note: put docker details in a single file, not individual files, otherwise millions of small files will hurt performance
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/12/07'


def docker_state_poll(outpath):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath) # i.e. 2020-12-07_12.10.33.007, we have removed space and colon
    
    # list running+stopped containers in docker 
    f = os.popen("sudo docker ps --all") # the output is a table
    lines = f.read().splitlines()        
    #keys = ['CONTAINER ID', 'IMAGE', 'COMMAND', 'CREATED', 'STATUS', 'PORTS', 'NAMES']
    keys = re.split(r'\s{2,}', lines[0])
    num = len(keys)
    indices = [lines[0].index(k) for k in keys]
    indices.append(None) # str[idx:None] will get the tail
    containers = list()
    container_details = list()

    for line in lines[1:]:
        # split line with the postion of keys, key-val is alligned
        vals = [line[indices[i]:indices[i+1]].strip() for i in range(num)]
        ctr = dict(list(zip(keys, vals)))
        containers.append(ctr)
        # inspect the detail of a container, don't write into an individual file (millions small files)
        f1 = os.popen("sudo docker inspect %(ctrId)s" % {'ctrId': vals[0]})
        ctr_detail = json.load(f1) # [{...}] a list with 1 element
        container_details.extend(ctr_detail)
    
    # write the docker-ps brief info
    with open('%s/docker_ps_out.json' % subpath, 'w') as fo:
        print(json.dumps(containers, sort_keys=True, indent=4), file=fo) # without indent can save space 
    # write the docker details info
    with open('%s/all_containers.json' % subpath, 'w') as fo1:
        print(json.dumps(container_details, sort_keys=True, indent=4), file=fo1)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    docker_state_poll(options.outpath)

if __name__ == '__main__':
    main()

