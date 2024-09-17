#!/usr/bin/env python

import os
import re
import json
from optparse import OptionParser
from utility import *

'''
polling the iptable rules in the node only once 
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/10'


def iptables_poll(outpath):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have remove space and colon in utility.py

    # 3-level hierarchy: table/chain/rule
    tablenames = ['filter', 'nat', 'mangle', 'raw', 'security']
    iptables = list()
    for tablename in tablenames:
        # list chain/rules in the table 
        f = os.popen("sudo iptables -t %s -L -n --line-numbers" % tablename) # line-number decides match priority  
        lines = f.read().splitlines()     
        
        # table 
        table = dict()
        table['table'] = tablename
        table['chains'] = list()
        iptables.append(table)
        
        for ln in lines:
            # chain line: Chain xxx (#reference/policy)
            if re.match(r'^Chain ', ln):
                re.sub('[()]', '', ln) # substitude brackets with space
                vals = ln.split()    
                chain = dict()
                chain['chain'] = vals[1]
                if vals[2] in ['ACCEPT', 'DROP']:
                    chain['policy'] = vals[2] # default policy for all rules in the chain 
                    chain['references'] = None
                if vals[3] == 'references':
                    chain['references'] = vals[2]
                    chain['policy'] = 'DROP'
                chain['rules'] = list()
                table['chains'].append(chain)
            # rule caption line: num,target,prot,opt,source,destination,comment(user add)
            elif re.match(r'^num ', ln):
                ruleKeys = ln.split() + ['comment']
                #rulekeys = ['target', 'prot', 'opt', 'source', 'destination', 'comment']
            # rule line: target,prot,opt,source,destination,comment vals 
            elif len(ln) > 0: # skip empty line
                ruleVals = re.split(r'\s{2,}', ln) 
                rule = dict(zip(ruleKeys, ruleVals))    
                chain['rules'].append(rule)    
    # write iptables out
    with open('%s/iptables_out.json' % subpath, 'w') as fo:
        print >> fo, json.dumps(iptables, sort_keys=True, indent=4) # with indent


def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    (options, args) = parser.parse_args()
    iptables_poll(options.outpath)

if __name__ == '__main__':
    main()

