#!/usr/bin/env python

import os
import re
import json
from optparse import OptionParser
from utility import *

'''
polling the current docker images in the node only once, not consider the docker-hub 
Note: put image details in a single file, otherwise millions small files will hurt performance
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/12/07'


def image_state_poll(outpath):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath)    
    
    # list images in the node 
    f = os.popen("sudo docker images") # the output is a table
    lines = f.read().splitlines()    
    #keys = ['REPOSITORY','TAG', 'IMAGE ID', 'CREATED', 'SIZE']    
    keys = re.split(r'\s{2,}', lines[0])
    num = len(keys)
    indices = map(lambda k: lines[0].index(k), keys)
    indices.append(None) # str[idx:None] will get the tail
    images = list()
    image_details = list()

    for line in lines[1:]:
        # split line with the postion of keys, key-val is alligned
        vals = [line[indices[i]:indices[i+1]].strip() for i in xrange(num)]
        img = dict(zip(keys, vals))
        images.append(img)
        # inspect the detail of an image
        f1 = os.popen("sudo docker inspect %(imgId)s" % {'imgId': vals[2]})
        image_details.extend(json.load(f1)) # f1 will auto closed after json.load()

    # write the docker-images brief info
    with open('%s/docker_images_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(images, sort_keys=True, indent=4) # without indent will save space

    with open('%s/all_images.json' % subpath, 'w') as fo1:
        print >> fo1, json.dumps(image_details, sort_keys=True, indent=4)


def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    image_state_poll(options.outpath)

if __name__ == '__main__':
    main()

