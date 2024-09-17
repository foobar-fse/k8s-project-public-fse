#!/usr/bin/env python


import os
import json
from optparse import OptionParser
from utility_3 import *

'''
poll calico resources in the cluster only once
it can run in a node or pod with calicoctl installed
'''

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/10'


def calico_resource_poll(outpath, format):
    # mkdir with timestamp 
    subpath = mkdir_with_timestamp(outpath)
    #calicoctl = "kubectl exec -i -n kube-system calicoctl -- /calicoctl"
    calicoctl='/usr/local/bin/calicoctl'

    # get all calico resource types, usually not change        
    fr = os.popen("%(calicoctl)s get -h | grep \* | awk '{print $2}' | sort" % {"calicoctl": calicoctl})
    calico_resources = fr.read().split('\n')[:-1]
    print(calico_resources)
    for resource in calico_resources:
    #calicoctl get $resource -o json > $datapath/$resource-out.json;
        cmd = "%(calicoctl)s get  %(resource)s  -o %(format)s > %(subpath)s/%(resource)s-out.%(format)s"\
            %{'calicoctl': calicoctl, 'resource': resource, 'format': format, 'subpath': subpath}
        print(cmd)
        os.system(cmd)

def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    parser.add_option('-f', '--format', metavar='format', default='json', help='yaml or json')
    (options, args) = parser.parse_args()
    calico_resource_poll(options.outpath, options.format)

if __name__ == '__main__':
    main() 
