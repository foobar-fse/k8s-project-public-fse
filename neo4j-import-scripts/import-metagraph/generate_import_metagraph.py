#!/usr/bin/env python

import sys
import os
#import stringcase

#neoPath = sys.argv[1]
#dataPath = sys.argv[2]
neo4jPath='/home/neo/neo4j-community-4.3.6'
dataPath='/home/neo/neo4j-import-metagraph2-total-2'

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

vertexFiles = sorted(os.listdir(nodePath))

categories = [x.split('.')[0] for x in vertexFiles]

# vertex shares header
#  --nodes=NativeEntity=$dataPath/headers/vertex_header.csv,$dataPath/nodes/nativeEntity.csv\
nodeHeader = '$dataPath/headers/vertex_header.csv'

for catg, vf in zip(categories, vertexFiles):
        nodeFile = '$dataPath/nodes/' + vf
        cmd = '  --nodes=' + catg + '=' + nodeHeader + ',' + nodeFile + '\\'
        print(cmd)


#--relationships=$dataPath/headers/edge/edge_header.csv,$dataPath/relations/edge.csv
# we have specified TYPE in edge_header.csv

relHeader = '$dataPath/headers/edge_header.csv'
relFile = '$dataPath/relations/edge.csv'
cmd = '  --relationships=' + relHeader + ',' + relFile # not need '\'
print(cmd)

print('\n')
