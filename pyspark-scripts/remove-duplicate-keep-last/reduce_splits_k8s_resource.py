'''
Reduce the output splits by remove_duplicate_keep_last, execute a second pass 
to eliminate any potential duplicates that may occur at split boundaries.
i.e, 2023-05 and 2023-06

'''

import sys
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: reduce_splits_k8s_resource [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonReduceSplitsK8sResource")\
        .getOrCreate()

    # set log level to 'WARN' 
    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    # read in the splits and drop tag fields
    resc = spark.read.json(pathIn)\
                .drop('dupe', 'withNext')
    
    '''
    # for compare two runs
    resc = spark.read.json(pathIn)\
                .withColumnRenamed('dupe', 'dupe1')\
                .withColumnRenamed('withNext', 'withNext1')
    '''   
    
    # window partition by kind
    w3 = Window().partitionBy('kind').orderBy(['metahash', 'timestamp'])

    # dupe for removing duplicates, withNext for last snapshot 
    resc2 = resc.withColumn('dupe', (col('metahash') == lag('metahash').over(w3))\
                                        & (col('status').eqNullSafe(lag('status').over(w3))))\
                    .withColumn('withNext', (col('metahash') == lead('metahash').over(w3))\
                                        & (col('timestamp') <= lead('timestamp').over(w3)))

    resc3 = resc2.filter((col("dupe") == False) | (col("dupe").isNull())\
                            | (col('withNext') == False) | (col('withNext').isNull()))

    resc3.write.json(pathOut)

    spark.stop()

