#!/bin/bash
 

dataPath=/home/neo/neo4j-import-metagraph2-total-2/

# copy edges to relations
python3 united_copy.py -s $dataPath/metaGraph/edge -d $dataPath/relations

# copy vertices to nodes
python3 united_copy.py -s $dataPath/metaGraph/vertex  -d $dataPath/nodes -m 'recursive'

# copy headers, we put all headers in the $dataPath/headers/

# header/edge
python3 united_copy.py -s $dataPath/metaGraph/header/edge -d $dataPath/headers -x '_header.csv'

# header/vertex
python3 united_copy.py -s $dataPath/metaGraph/header/vertex -d $dataPath/headers -x '_header.csv'

