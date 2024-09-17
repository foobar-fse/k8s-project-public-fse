#!/usr/bin/env python

import sys
import os
#import stringcase

#neoPath = sys.argv[1]
#dataPath = sys.argv[2]
neo4jPath='/home/neo/neo4j-community-4.3.6'
dataPath='/home/neo/neo4j-import-graphframe-total-2'

print('#!/bin/bash\n')

print('neo4jPath=' + neo4jPath)
print('dataPath=' + dataPath)
print('\n')

print('$neo4jPath/bin/neo4j-admin import --database=neo4j \\')
print('  --delimiter=\";\" --array-delimiter=\"|\" --quote=\"\'\" --bad-tolerance=10000 \\')
print('  --skip-duplicate-nodes --skip-bad-relationships \\')

nodePath = dataPath + '/nodes'
relationPath = dataPath + '/relations'
headerPath = dataPath + '/headers'
srcVertexFiles = sorted(os.listdir(nodePath + '/srcVertex'))
destVertexFiles = sorted(os.listdir(nodePath + '/destVertex'))

srcKinds2 = [x.split('.')[0] for x in srcVertexFiles]
destKinds2 = [x.split('.')[0] for x in destVertexFiles]

'''
srcKinds2 = [stringcase.capitalcase(x) for x in srcKinds] # use capitalcase for src-vertex kind
destKinds2 = [] # use camelcase for dest-vertex tags
for x in destKinds:
    if x.isupper():
        destKinds2.append(x.lower())
    else:
        destKinds2.append(stringcase.camelcase(stringcase.snakecase(x)))
'''

#  --nodes=APIService=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/APIService.csv\

# src vertices share the same src-header
srcHeader = '$dataPath/headers/srcVertex/k8s-native_header.csv'
for sk,sf in zip(srcKinds2, srcVertexFiles):
        srcNodeFile = '$dataPath/nodes/srcVertex/' + sf
        cmd = '  --nodes=' + sk + '=' + srcHeader + ',' + srcNodeFile + '\\'
        #print(sk, sf)
        print(cmd)

# dest vertices have different dest-headers
for dk,df in zip(destKinds2, destVertexFiles):
        destNodeFile = '$dataPath/nodes/destVertex/' + df
        destHeader = '$dataPath/headers/destVertex/' + dk + '_header.csv'
        cmd = '  --nodes=' + dk + '=' + destHeader + ',' + destNodeFile + '\\'
        print(cmd)
        #print(dk, df)

#--relationships=$dataPath/headers/edge/edge_header.csv,$dataPath/relations/edge.csv
# we have specified TYPE in edge_header.csv

relHeader = '$dataPath/headers/edge/edge_header.csv'
relFile = '$dataPath/relations/edge.csv'
cmd = '  --relationships=' + relHeader + ',' + relFile # not need '\'
print(cmd)

