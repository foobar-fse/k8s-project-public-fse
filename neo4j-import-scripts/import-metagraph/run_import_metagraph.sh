#!/bin/bash

neo4jPath=/home/neo/neo4j-community-4.3.6
dataPath=/home/neo/neo4j-import-metagraph2-total-2


$neo4jPath/bin/neo4j-admin import --database=neo4j \
  --delimiter=";" --array-delimiter="|" --quote="'" --bad-tolerance=10000 \
  --skip-duplicate-nodes --skip-bad-relationships \
  --nodes=ExternalEntity=$dataPath/headers/vertex_header.csv,$dataPath/nodes/ExternalEntity.csv\
  --nodes=NativeEntity=$dataPath/headers/vertex_header.csv,$dataPath/nodes/NativeEntity.csv\
  --nodes=Snapshot=$dataPath/headers/vertex_header.csv,$dataPath/nodes/Snapshot.csv\
  --relationships=$dataPath/headers/edge_header.csv,$dataPath/relations/edge.csv


