#!/usr/bin/env python

'''
polling k8s api resources in the cluster only once. 
kubectl can run in k8s-client, not required inside the k8s-cluster
# Note: we put all resources in a single file, not ~120 individual files
'''

import os
import json
from optparse import OptionParser
from utility import *

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.2'
__DATE__ = '2020/12/08'


def k8s_apiresource_poll(outpath, format):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath) # we have remove space and colon in utility.py

    # get all api-resource types, usually not change, unless the k8s version changes    
    fr = os.popen("kubectl  api-resources  | awk '{print $1}' | grep -v NAME  | sort")
    res_types = fr.read().split('\n')[:-1]
    print res_types
    
    resources = list()
    # get each resource in yaml/json format, and put in a single json file
    for res_type in res_types:
        cmd = "kubectl get %(res_type)s --all-namespaces -o %(format)s" % {'res_type': res_type, 'format': format}
        print cmd
        res = json.load(os.popen(cmd))
        resources.append(res)
    with open('%s/all_resources.json' % subpath, 'w') as fw:
        print >> fw, json.dumps(resources, sort_keys=False, indent=4) # we don't need to sort keys


def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    parser.add_option('-f', '--format', metavar='format', default='json', help='yaml or json')
    (options, args) = parser.parse_args()
    k8s_apiresource_poll(options.outpath, options.format)

if __name__ == '__main__':
    main()

