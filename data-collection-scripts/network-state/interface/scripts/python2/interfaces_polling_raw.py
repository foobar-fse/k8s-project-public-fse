#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility import *

'''
polling the interfaces with ifconfig in the node only once
keep the shell output without parsing 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/10'


def interface_state_poll(outpath):
    #mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath)
    subpath2 = "\ ".join(subpath.split()) # shell command requires path without space.    

    # list interfaces in the node
    os.system('ifconfig > %s/interfaces_out.txt' % subpath2)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    interface_state_poll(options.outpath)

if __name__ == '__main__':
    main()

