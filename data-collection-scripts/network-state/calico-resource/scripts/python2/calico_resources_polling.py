#!/usr/bin/env python


import os
import json
from optparse import OptionParser
from utility import *

'''
poll calico resources in the cluster only once
it can run in a node or pod with calicoctl installed
Note: we put all resources in a single json file
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/12/08'


def calico_resource_poll(outpath, format):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath)
    calicoctl = "kubectl exec -i -n kube-system calicoctl -- /calicoctl" # use calicoctl pod

    # get all calico resource types, usually not change        
    fr = os.popen("%(calicoctl)s get -h | grep \* | awk '{print $2}' | sort" % {"calicoctl": calicoctl})
    res_types = fr.read().split('\n')[:-1]
    print res_types

    resources = list()
    for res_type in res_types:
        #calicoctl get $resource -o json > $datapath/$resource-out.json;
        cmd = "%(calicoctl)s get %(res_type)s -o %(format)s" % {'calicoctl': calicoctl, 'res_type': res_type, 'format': format}
        print cmd
        res = json.load(os.popen(cmd))
        resources.append(res)
    # write to a single json file
    with open('%s/all_calico_resources.json' % subpath, 'w') as fw:
        print >> fw, json.dumps(resources, sort_keys=False, indent=4) # we don't sort keys

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    parser.add_option('-f', '--format', metavar='format', default='json', help='yaml or json')
    (options, args) = parser.parse_args()
    calico_resource_poll(options.outpath, options.format)

if __name__ == '__main__':
    main() 
