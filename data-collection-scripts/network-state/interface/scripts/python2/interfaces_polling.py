#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility import *

'''
polling the interfaces with ifconfig in the node only once 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/10'


def interface_state_poll(outpath):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath) # we have removed space and colon in utility.py
    
    # list interfaces in the node 
    f1 = os.popen("ifconfig") #
    blocks = f1.read().split('\n\n')[:-1] # each interface separated by a blank line
    interfaces = list()
    for blk in blocks:
        lines = blk.splitlines()
        iface = dict()
        parts = lines[0].split(':') # eth0: flags=4163<>  mtu 1400
        iface['interface'] = parts[0]                
        lines[0] = parts[1].replace('=', ' ') # flags=4163<xxx> mtu 1400
        #map(lambda l: l.strip(), lines) # strip leading space
        #details: status, ipv4, ipv6, link_encap+mac,  mtu
        for ln in lines:
            ln = ln.strip() # strip leading space
            splits = ln.split()
            if splits[0] == 'flags': # status
                iface['flags'] = splits[1]
                iface['mtu'] = splits[3] # we may ignore it
            elif splits[0] == 'inet': # ipv4
                keys = splits[::2]
                vals = splits[1::2]    
                iface['ipv4'] = dict(zip(keys, vals)) # inet, netmast, [broadcast]
            elif splits[0] == 'inet6': #ipv6
                keys = splits[::2]
                vals = splits[1::2]
                iface['ipv6'] = dict(zip(keys, vals)) # inet6, prefixlen, scopeid
            elif splits[0] in ['ether', 'loop', 'tunnel']: # link encap 
                iface['type'] = splits[0]
                if splits[0] == 'ether':
                    iface['mac'] = splits[1]
                    iface['txqueuelen'] = splits[3] # we may ignore it
                else:
                    iface['mac'] = None
                    iface['txqueuelen'] = splits[2]
    
            # statistics
            elif splits[0] == 'RX' or splits[0] == 'TX':
                iface['statistics'] = list()
                if splits[1] == 'packets': #RX/TX packets 7809968  bytes 548732129 (523.3 MiB)
                    splits = splits[:4] # remove last comment
                # RX errors 0  dropped 0  overruns 0  frame 0, TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
                keys = map(lambda k: splits[0] + ' ' + k, splits[1::2])        
                vals = splits[2::2]
                iface['statistics'].append(dict(zip(keys, vals)))
        #print iface
        interfaces.append(iface)
    # write the docker-images brief info
    with open('%s/interfaces_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(interfaces, sort_keys=True, indent=4) # without indent can save space

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    interface_state_poll(options.outpath)

if __name__ == '__main__':
    main()

