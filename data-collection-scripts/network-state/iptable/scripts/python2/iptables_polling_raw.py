#!/usr/bin/env python

import os
import json
from optparse import OptionParser
from utility import *

'''
polling the iptable rules in the node only once and keep the shell output without parsing.
we test the output format of ubuntu and centos are the same. so parsing is ok. 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/10'


def iptables_poll(outpath):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have removed space and colon in utility.py
    
    # 3-level hierarchy: table/chain/rule
    tablenames = ['filter', 'nat', 'mangle', 'raw', 'security']
    iptables = list()
    for tablename in tablenames:
        # list chain/rules in the table, the line-number decides match priority  
        cmd = "sudo iptables -t %(tablename)s -L -n --line-numbers > %(subpath)s/%(tablename)s_table_out.txt"\
            % {'subpath': subpath, 'tablename': tablename}
        print cmd
        os.system(cmd)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    iptables_poll(options.outpath)

if __name__ == '__main__':
    main()

