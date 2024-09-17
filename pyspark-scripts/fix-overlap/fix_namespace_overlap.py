# fix the overlap of edge in stategraph, currently only handle metadata_namespace 
# this will only drop some edges in stategraph, but not affect metagraph

import sys
from graphframes import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def check_overlap(tmin, tmax, Tmin, Tmax):
    if ((Tmax is None) and (Tmin is None)):
        return True
    elif ((tmin <= Tmax) and (tmax >= Tmin)):
        return True
    else:
        return False

check_overlap_udf = udf(check_overlap, BooleanType())


# main function
if __name__ == "__main__":
    """
        Usage: fix_overlap.py [graphPathIn] [graphPathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonFixOverlap")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    graphPathIn = sys.argv[1]
    graphPathOut = sys.argv[2]
   
    print('fix tmin > tmax ...')
    # read in edges, and fix the 'tmin > tmax' problem
    # We find only 'metadata_namespace' will break the rule
    # tmin >= Tmin and tmax <= Tmax
    # which indicate the mistakes in Namespace
    edges = spark.read.json(graphPathIn + '/edges')
    edges2 = edges.withColumn("tmin_swap", when(col("tmin") > col("tmax"), col("tmax")).otherwise(col("tmin"))) \
                        .withColumn("tmax_swap", when(col("tmin") > col("tmax"), col("tmin")).otherwise(col("tmax"))) \
                        .drop("tmin") \
                        .drop("tmax") \
                        .withColumnRenamed("tmin_swap", "tmin") \
                        .withColumnRenamed("tmax_swap", "tmax")
    
    print('calculate Tmin and Tmax for dest entity ...')
    # etr stands for entity time range
    etr = edges2.filter(col('type').isin(['HasState', 'HasEvent']))\
                .groupBy('src')\
                .agg(min('tmin').alias('Tmin'), max('tmax').alias('Tmax'))\
                .withColumnRenamed('src', 'dst')

    print('fix overlap problems in metadata_namespace ...')
    ns = edges2.filter((col('key') == 'metadata_namespace'))
    ns2 = ns.join(etr, 'dst', 'left')\
            .withColumn('overlap', check_overlap_udf('tmin', 'tmax', 'Tmin', 'Tmax'))\

    # if all 'True', we keep all, for different time range
    # if only 'False', we keep the last one
    # if mixed 'True' and 'False', we discard the 'False'
    wd = Window().partitionBy('src').orderBy('overlap')
    ns3 = ns2.withColumn('keep', when(col('overlap') == True, True)\
                                .when((col('overlap') == False) & (lead('overlap').over(wd).isNull()), True)\
                                .otherwise(False))\
            .filter(col('keep') == True)

    print('tight the tmin and tmax if possible ...')
    # tight the [tmin, tmax] if possible
    ns4 = ns3.withColumn('tight', when((col('Tmin') >= col('tmin')) & (col('Tmax') <= col('tmax')), True).otherwise(False))\
            .withColumn('tmin2', when(col('tight') == True, col('Tmin')).otherwise(col('tmin')))\
            .withColumn('tmax2', when(col('tight') == True, col('Tmax')).otherwise(col('tmax')))\
            .drop('tmin', 'tmax')\
            .withColumnRenamed('tmin2', 'tmin')\
            .withColumnRenamed('tmax2', 'tmax')

    print('merge fixed edges ...')
    # select names from edges2  and merge as new edges
    ns5 = ns4.select(edges2.schema.names)
    edges3 = edges2.filter(col('key') != 'metadata_namespace').union(ns5)

    print('write new edges to hdfs ...')
    edges3.write.json(graphPathOut + '/fixed-edges')

    spark.stop()

