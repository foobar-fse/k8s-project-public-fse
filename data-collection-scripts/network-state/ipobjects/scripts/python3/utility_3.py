#!/usr/bin/env python

import os
import json
import socket
import datetime



def mkdir_with_timestamp(outpath):
    #get node hostname and IP, usually not change. if changed, some bugs may appear  
    nodename = socket.gethostname()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #socket.gethostbyname not work, return 127.0.0.1
    s.connect(("8.8.8.8", 80))
    nodeIP = s.getsockname()[0]
    s.close()

    # get current time in UTC
    utctime = datetime.datetime.utcnow()
    timestamp = utctime.strftime('%Y-%m-%d %H:%M:%S.%3d')
    aux = dict()
    aux['nodename'] = nodename
    aux['nodeIP'] = nodeIP
    aux['timestamp'] = timestamp
    print(aux)

    # create sub-directory with datetime
    pathtime = utctime.strftime('%Y-%m-%d_%H.%M.%S.%3d') # hdfs not support space and colon in path 
    subpath = outpath + "/" + pathtime
    if not os.path.exists(subpath): # like 'mkdir -p', if two processes try to create a same directory
        os.mkdir(subpath) # python can create subpath with space.     

    # write the aux-info 
    with open('%s/aux_node_timestamp.json' % subpath, 'w') as fa:
        print(json.dumps(aux, sort_keys=True, indent=4), file=fa) # with indent 

    return subpath
