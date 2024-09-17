# we convert the metagraph build by graphframe from json to csv for neo4j
# note: with category info for vertex, we have to split the vertices by category

import sys
import json
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# get header for edge and vertex
# vertex: ['id', 'kind'] or ['id', 'kind', 'category']
# edge: ['srcKind', 'destKind', 'key', 'type', 'isReverse', 'count', 'src', 'dst']

def get_header(ks, delimiter=';', mode='vertex'):
    res = ''
    if mode == 'vertex':
        for x in ks:
            if x == 'id':
                x += ':ID'
            res = res + delimiter + x
    elif mode == 'edge':
        for x in ks:
            if x == 'src':
                x += ':START_ID'
            elif x == 'dst':
                x += ':END_ID'
            elif x == 'type':
                x += ':TYPE'
            res = res + delimiter + x
    else:
        print('mode can only be vertex or edge')
        exit(1)
    
    return res[1:] # remove the leading delimiter


# main function
if __name__ == "__main__":
    """
        Usage: convert_metagraph_neo4j_csv.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonConvertMetagraphNeo4jCSV")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # pathIn = testPath + '/metaGraph_json'
    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    # meta edges share the same schema
    print('convert edges/* to csv ...')

    edge = spark.read.json(pathIn + '/edges')
    # we use overwrite for convenience with deleting in hdfs
    edge.repartition(1).write.options(delimiter=';').csv(pathOut + '/edge/')
    
    # get header for edge
    edgeHeader = get_header(edge.schema.names, mode='edge')
    spark.sparkContext.parallelize([edgeHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/edge/')

    # meta vertices also share the same schema
    print('convert vertices/* to csv ...')

    vertex = spark.read.json(pathIn + '/vertices')
    categories = vertex.select('category').distinct()\
                    .rdd.map(lambda x: x['category'])\
                    .collect()

    for catg in categories:
        vertex.filter(col('category') == catg)\
                .repartition(1).write.options(delimiter=';').csv(pathOut + '/vertex/' + catg) 

    # get header for vertex
    vertexHeader = get_header(vertex.schema.names)
    spark.sparkContext.parallelize([vertexHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/vertex')

    spark.stop()

