#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility import *

'''
polling the 'ip [OBJECT]' (i.e, route, link, rule)in the node only once

several OBJECT from the following list can be polled by 'ip' command
{ link | address | addrlabel | route | rule | neigh | ntable | tunnel | tuntap | maddress | mroute | mrule | monitor | xfrm | netns | l2tp | fou | macsec |tcp_metrics | token | netconf | ila | vrf | sr | nexthop }

'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2022/12/17'


def ipobjects_poll(outpath):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have remove space and colon in utility.py

    #objNames = ['link', 'route']
    objNames = ['link', 'address', 'addrlabel', 'route', 'rule', 'neigh', 'ntable', 'tunnel', ' maddress', 'mrule', 'tcp_metrics', 'token', 'netconf']

    ipObjects = list()
    for objName in objNames:
        f = os.popen('ip -json -pretty %s' % objName) # -json and -pretty can out JSON format
        ipObj = dict()
        ipObj['object'] = objName
        ipObj['content'] = json.load(f)
        ipObjects.append(ipObj)

    with open('%s/ipobjects_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(ipObjects, sort_keys=True, indent=4) # with indent

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    ipobjects_poll(options.outpath)

if __name__ == '__main__':
    main()

