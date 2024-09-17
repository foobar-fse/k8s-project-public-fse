#!/bin/bash
 

dataPath=/home/neo/neo4j-import-graphframe-total-2/

# note: keep the deepest directory

# copy edges to relations
python3 united_copy.py -s $dataPath/entityGraph/edge -d $dataPath/relations

# copy vertices to nodes
# copy srcVertex/k8s-native/*
python3 united_copy.py -s $dataPath/entityGraph/srcVertex/k8s-native  -d $dataPath/nodes/srcVertex -m 'recursive' 

# copy destVertex/k8s-nfs-snapshot/*
python3 united_copy.py -s $dataPath/entityGraph/destVertex/k8s-nfs-snapshot/ -d $dataPath/nodes/destVertex -m 'recursive'

# copy other destVertex/*
python3 united_copy.py -s $dataPath/entityGraph/destVertex/ -d $dataPath/nodes/destVertex -m 'recursive'


# copy headers, we put all headers in the $dataPath/headers/

# header/edge
python3 united_copy.py -s $dataPath/entityGraph/header/edge -d $dataPath/headers/edge -x '_header.csv'

# k8s-native share the same schema/header, and we rename it as src_header.csv
python3 united_copy.py -s $dataPath/entityGraph/header/srcVertex/k8s-native/ -d $dataPath/headers/srcVertex/ -x '_header.csv'

#mv $dataPath/headers/k8s-native_header.csv $dataPath/headers/src_header.csv

python3 united_copy.py -s $dataPath/entityGraph/header/destVertex/ -d $dataPath/headers/destVertex -x '_header.csv' -m 'recursive'

python3 united_copy.py -s $dataPath/entityGraph/header/destVertex/k8s-nfs-snapshot/  -d $dataPath/headers/destVertex -x '_header.csv'  -m 'recursive'
