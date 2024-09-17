#!/usr/bin/env python

'''
polling k8s api resources in the cluster only once. 
kubectl can run in k8s-client, not required inside the k8s-cluster
'''

import os
import json
from optparse import OptionParser
from utility import *

__AUTHOR__ = 'anonymous'
__VERSION__ = '1.1'
__DATE__ = '2020/09/09'


def k8s_apiresource_poll(outpath, format):
    # mkdir with timestamp
    subpath = mkdir_with_timestamp(outpath)
    subpath2 = "\ ".join(subpath.split()) # shell command requires path without space.  

    # get all api-resource types, usually not change, unless the k8s version changes    
    fr = os.popen("kubectl  api-resources  | awk '{print $1}' | grep -v NAME  | sort")
    api_resources = fr.read().split('\n')[:-1]
    print api_resources

    # get each resource in yaml/json format
    for resource in api_resources:
        #kubectl get $resource --all-namespaces -o json > $datapath/$resource-out.json;
        cmd = "kubectl get  %(resource)s  --all-namespaces -o %(format)s > %(subpath2)s/%(resource)s-out.%(format)s"\
             %{'resource': resource, 'format': format, 'subpath2': subpath2}
        print cmd
        os.system(cmd)


def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-o', '--outpath', metavar='outpath', default='./', help='base directory for output')
    parser.add_option('-f', '--format', metavar='format', default='json', help='yaml or json')
    (options, args) = parser.parse_args()
    k8s_apiresource_poll(options.outpath, options.format)

if __name__ == '__main__':
    main()

