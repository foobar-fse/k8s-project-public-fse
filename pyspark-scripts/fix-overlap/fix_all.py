#!/usr/bin/env python

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


def check_overlap(tmin, tmax, Tmin, Tmax):
        if ((Tmax is None) and (Tmin is None)):
                return True
        elif ((tmin <= Tmax) and (tmax >= Tmin)):
                return True
        else:
                return False

check_overlap_udf = udf(check_overlap, BooleanType())



total_edges2 = total_edges.withColumn("tmin_swap", when(col("tmin") > col("tmax"), col("tmax")).otherwise(col("tmin"))) \
                        .withColumn("tmax_swap", when(col("tmin") > col("tmax"), col("tmin")).otherwise(col("tmax"))) \
                        .drop("tmin") \
                        .drop("tmax") \
                        .withColumnRenamed("tmin_swap", "tmin") \
                        .withColumnRenamed("tmax_swap", "tmax")

# We find only 'metadata_namespace' will break the rule
# tmin >= Tmin and tmax <= Tmax
# which indicate the mistakes in Namespace

entity_time_range = total_edges2.filter(col('type').isin(['HasState', 'HasEvent'])).groupBy('src').agg(min('tmin').alias('Tmin'), max('tmax').alias('Tmax')).cache()

etr1 = entity_time_range.select('src', col('Tmin').alias('Tmin1'), col('Tmax').alias('Tmax1'))
etr2 = entity_time_range.select(col('src').alias('dst'), col('Tmin').alias('Tmin2'), col('Tmax').alias('Tmax2'))

entity_entity_edges = total_edges.filter(col('type').isin(['ReferInternal', 'UseExternal'])).cache()


entity_entity_edges2 = entity_entity_edges.join(etr1, 'src', 'left').join(etr2, 'dst', 'left').cache()

# we find all overlap1 are 'True', while some overlap2 are 'False'
# we induce the edge from srcKind to destKind, therefore tmin >= Tmin1 and tmax <= Tmax1, and overlap1 will always be 'True'
# so we can only check overlap2
entity_entity_edges3 = entity_entity_edges2.withColumn('overlap1', check_overlap_udf('tmin', 'tmax', 'Tmin1', 'Tmax1')).withColumn('overlap2', check_overlap_udf('tmin', 'tmax', 'Tmin2', 'Tmax2')).cache()


w4 = Window().partitionBy('src').orderBy('overlap2')

# take namespace for example
tt = entity_entity_edges3 
ns  = tt.filter( (col('key') == 'metadata_namespace')).cache()

# if all 'True', we keep all, for different time range
# if only 'False', we keep the last one
# if mixed 'True' and 'False', we discard the 'False'

ns2 = ns.withColumn('keep', when(col('overlap2') == True, True)\
                        .when((col('overlap2') == False) & (lead('overlap2').over(w4).isNull()), True)\
                        .otherwise(False))




