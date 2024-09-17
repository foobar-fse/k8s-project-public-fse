##
import uuid
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#sysKind = ['APIService', 'BlockAffinity', 'ClusterInformation', 'ClusterRole', 'ClusterRoleBinding', 'ComponentStatus', 'CustomResourceDefinition', 'FelixConfiguration', 'IPAMBlock', 'IPAMHandle', 'IPPool', 'MeshPolicy', 'MutatingWebhookConfiguration', 'Namespace', 'Node', 'PersistentVolume', 'PriorityClass', 'StorageClass', 'ValidatingWebhookConfiguration']

#nativeTag = spark.read.csv(testPath + '/k8s-native.csv', header=True)
#nativeResc = rescFlatDF3.join(nativeTag, ['kind', 'key'], 'inner')

def get_k8s_native(nativeResc):
    # the 'kind' of referenced entity (as destKind) is 'kind2', and it is different from the source 'kind' (as srcKind)
    nativeKind = nativeResc.filter(col('tag2') == 'kind').withColumnRenamed('val', 'kind2').cache()
    nativeName = nativeResc.filter(col('tag2') == 'name').withColumnRenamed('val', 'name2').cache()
    nativeNamespace = nativeResc.filter(col('tag2') == 'namespace').withColumnRenamed('val', 'namespace2').cache()
    # we omit ComponentStatus which has null-uid, therefore not included in the k8s-native.csv
    # we exclude uid2 not with uuid-pattern, examples occurs in Event involvedObject.uid
    uuidPtn = '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    nativeUid = nativeResc.filter((col('tag2') == 'uid') & col('val').rlike(uuidPtn)).withColumnRenamed('val', 'uid2').cache()
    nativeSelfLink = nativeResc.filter(col('tag2') == 'selfLink').withColumnRenamed('val', 'selfLink2').cache()
    nativeMetaNamespace = nativeResc.filter(col('tag2') == 'metaNamespace').withColumnRenamed('val', 'metaNamespace2').cache()

    ## metadata contains [kind, name, namespace, uid, selfLink]
    # we don't have a metadata_kind field, the 'kind' is implicitly included in each row
    metaUid = nativeUid.filter(col('tag') == 'metadata') # include uid and kind 
    metaName = nativeName.filter(col('tag') == 'metadata').select('metadataUid', 'timestamp', 'prefix', 'name2')
    metaNamespace = nativeNamespace.filter(col('tag') == 'metadata').select('metadataUid', 'timestamp', 'prefix', 'namespace2')
    metaSelfLink = nativeSelfLink.filter(col('tag') == 'metadata').select('metadataUid', 'timestamp', 'prefix', 'selfLink2')

    # we can't inner join on namespace, because 'Node' has no namespace
    # metaEntity has 4902903 rows
    metaEntity = metaUid.join(metaName, ['metadataUid', 'timestamp', 'prefix'], 'inner')\
                        .join(metaSelfLink, ['metadataUid', 'timestamp', 'prefix'], 'inner')\
                        .join(metaNamespace, ['metadataUid', 'timestamp', 'prefix'], 'left')\
                        .withColumn('kind2', col('kind'))
    
    # define the result keylist, we take metaEntity as the reference
    resKeyList = metaEntity.schema.names
    
    # knownObject is 37417, and original metaObject (directly select from rescDF) is 37420
    # the 3-difference is ComponentStatus
    '''
    knownObject = metaEntity.select('uid2', 'name2', 'namespace2', 'kind2', 'selfLink2').distinct().cache()
    '''
    # objects may share name2+namespace2+kind2, but differ in uid2, we prepare time-range for later join with name2 in vagueEntity
    # k8s ensures that at the same time a name is used by only one object, therefore, [(tmin, tmax), ...] will not overlap. (Confirmed)
    # only the objects from metaEntity will have a corresponding State/Event vertex
    knownObject2 = metaEntity.select('uid2', 'name2', 'namespace2', 'kind2', 'selfLink2', 'timestamp').distinct()\
                            .groupBy(['uid2', 'name2', 'namespace2', 'kind2', 'selfLink2'])\
                            .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))\
                            .cache()

    # get user-level resource kind for later use
    # sysKind requires namespace to be purely null, while userKind usually has non-null namespace
    # the following select has little bug (if userKind with null-namespace, which can not be distinguished), but it does not matter
    userKind = knownObject2.select('kind2', 'namespace2')\
                        .filter(col('namespace2').isNotNull())\
                        .select('kind2')\
                        .distinct()\
                        .rdd.map(lambda x: x.kind2)\
                        .collect()

    ## claimRef, involvedObject, targetRef contains [kind, name, namespace, uid]
    ## ownerRef contains [kind, name, uid], without namespace
    ## they can be joined on uid, and are clear to identify themselves
    # otherUidTag = ['claimRef', 'involvedObject', 'targetRef', 'ownerRef']
    
    withUidTag = nativeUid.select('tag').distinct().rdd.map(lambda x: x.tag).collect()
    otherUidTag = [x for x in withUidTag if x != 'metadata']
    clearUid = nativeUid.filter(col('tag').isin(otherUidTag))
    clearKind = nativeKind.filter(col('tag').isin(otherUidTag)).select('metadataUid', 'timestamp', 'prefix', 'kind2')
    clearName = nativeName.filter(col('tag').isin(otherUidTag)).select('metadataUid', 'timestamp', 'prefix', 'name2')
    clearNamespace = nativeNamespace.filter(col('tag').isin(otherUidTag)).select('metadataUid', 'timestamp', 'prefix', 'namespace2')
    clearEntity = clearUid.join(clearKind, ['metadataUid', 'timestamp', 'prefix'], 'inner')\
                    .join(clearName, ['metadataUid', 'timestamp', 'prefix'], 'inner')\
                    .join(clearNamespace, ['metadataUid', 'timestamp', 'prefix'], 'left')

    # tmpObject is not distinct on uid2, because ownerRef (null-namespace2) may share uid2 with others (i.e, involvedObject)
    # bug: the not-joined rows in clearObject may share uid2, but namespace2 is 'xxx' and null
    # tmpObject = clearEntity.select('uid2', 'name2', 'namespace2', 'kind2').distinct()

    # we will groupBy uid2 to make each row distinct on uid2 (no null-namespace2 together with non-null-namespace2)
    # note: there still can be uid2 with null-namespace2, i.e, ownerReferences has no namespace field 
    # we will join with knownObject to fill the missing namespace2
    tmpObject2 = clearEntity.select('uid2', 'name2', 'namespace2', 'kind2', 'timestamp')\
                            .groupBy('uid2')\
                            .agg(first('name2', ignorenulls=True).alias('name2'),\
                                    first('namespace2', ignorenulls=True).alias('namespace2'),\
                                    first('kind2', ignorenulls=True).alias('kind2'),\
                                    min('timestamp').alias('tmin'),\
                                    max('timestamp').alias('tmax'))

    # clearObject is distinct on uid2
    # clearObject still has lines with null namespace (i.e, 44 Service, 42 Ingress), because knownObject does not contain these lines
    # other null namespace line are system resource, i.e, 24 Node, 1 Namespace
    '''
    clearObject = tmpObject.join(knownObject.select('uid2', col('namespace2').alias('namespace3'), 'selfLink2'), 'uid2', 'left')\
                        .withColumn('namespace2', when(col('namespace3').isNotNull(), col('namespace3'))\
                                                    .otherwise(col('namespace2')))\
                        .drop('namespace3')\
                        .distinct() # fewer than tmpObject
    '''
    # we keep the tmin+tmax from tmpObject2, and later merge with knownObject2
    # bug: clearObject2 contains namespace2=null for user-level resource, i,e, Job, Service
    # which leads to many uncertainty in later join with vagueEntity, so we filter out these rows
    # we require [name2, namespace2, kind2] are clearly determined for clearObject2
    clearObject2 = tmpObject2.join(knownObject2.select('uid2', col('namespace2').alias('namespace3'), 'selfLink2'), 'uid2', 'left')\
                        .withColumn('namespace2', when(col('namespace3').isNotNull(), col('namespace3'))\
                                                    .otherwise(col('namespace2')))\
                        .drop('namespace3')\
                        .filter((col('namespace2').isNull() & col('kind2').isin(userKind)) == False)\
                        .distinct() # fewer than tmpObject
    

    # join back with clearEntity
    # the unclear rows (with user-level namespace as null) remain in clearEntity
    # as we use left-join, these rows exluded in clearObject2 will not affect 
    # clearEntityRes has 4655191 rows
    clearEntityRes = clearEntity.drop('namespace2')\
                                .join(clearObject2.select('uid2', 'namespace2', 'selfLink2'), 'uid2', 'left')\
                                .select(resKeyList)
    
    # merge knownObject2 and clearObject2, then extend the [tmin, tmax] of knownObject2
    # the time-ranges can be overlaped after extending. how about not extending? can we guarantee overlap-free? (Can not!)
    # whether or not extend, it will NOT affect the choice of uid2 accourding to name2 in later vagueEntity 
   
    t1 = knownObject2.union(clearObject2.select(knownObject2.schema.names)).distinct()
    t2 = t1.groupBy('uid2').agg(min('tmin').alias('tmin'), max('tmax').alias('tmax'))
    knownObject2 = t1.drop('tmin', 'tmax').join(t2, ['uid2'], 'inner').distinct().cache()
    
    # knownObject2 contains 42+1 rows which equal to its next, and will introduce unclarity in later join
    # but we have to tolerate it, since we can't distinguish them with our polling method
    # the 5-min polling timestamp can not distinguish events inside it 

    ## we precess all tags without uid in uniform way, these entities are vague or ambiguous
    ## subject contains [name, namespace, kind]
    ## objectRef, roleRef, scaleTargetRef contains [name, kind], 
    ## the other tagKind contains [name] or [name, namespace]
    # withUidTag = ['metadata', 'claimRef', 'involvedObject', 'targetRef', 'ownerRef']
    
    vagueName = nativeName.filter(col('tag').isin(withUidTag) == False).withColumnRenamed('name2', 'name1').distinct()
    vagueKind = nativeKind.filter(col('tag').isin(withUidTag) == False)\
                        .select('metadataUid', 'timestamp', 'prefix', col('kind2').alias('kind1'))\
                        .distinct()
    vagueNamespace = nativeNamespace.filter(col('tag').isin(withUidTag) == False)\
                            .select('metadataUid', 'timestamp', 'prefix', col('namespace2').alias('namespace1'))\
                            .distinct()
    # prefix for metaNamespace is 'metadata', and it is not needed 
    vagueMetaNamespace = nativeMetaNamespace.filter(col('tag').isin(withUidTag) == False)\
                        .select('metadataUid', 'timestamp', col('metaNamespace2').alias('metaNamespace1'))\
                        .distinct() # required
    vagueEntity = vagueName.join(vagueKind, ['metadataUid', 'timestamp', 'prefix'], 'left')\
                        .join(vagueNamespace, ['metadataUid', 'timestamp', 'prefix'], 'left')\
                        .join(vagueMetaNamespace, ['metadataUid', 'timestamp'], 'left')
    
    # complement kind1 with tagKind
    s1 = vagueEntity.withColumn('kind1', when(col('kind1').isNull(), col('tagKind')).otherwise(col('kind1')))

    # complement namespace1 with metaNamespace1 (to verify)
    # s2 has 1741717 rows
    s2 = s1.withColumn('namespace1', when(col('namespace1').isNull() & (col('kind1').isin(userKind)),\
                                                col('metaNamespace1'))\
                                    .otherwise(col('namespace1')))
    
    # strictly join on [kind, name, namespace] can still result in multiple uid2
    sx2 = s2.join(knownObject2, (s2.kind1 == knownObject2.kind2)\
                        & (s2.name1 == knownObject2.name2)\
                        & (s2.namespace1.eqNullSafe(knownObject2.namespace2)),\
                        'left')

    # if matched, there can be multiple uid2 for a single uid5
    # policy: choose the nearest-history uid2 if possible, otherwise, choose the nearest-future uid2; 
    # for uncertain rows, randomly choose a uid2
    
    # we add uid5 as a helper field
    udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())
    s3 = sx2.filter(col('uid2').isNotNull())\
            .withColumn('uid5', udf_uuid5(concat_ws('/', 'name2', 'namespace2', 'kind2')))
    
    # find the largest tmin which is smaller than timestamp (nearest-history)
    # a single record (or line) can be determined by [metadataUid, key, prefix, timestamp] (think about how we flatten fields)
    # don't only partitionBy [uid5, timestamp], which is not sufficient for s3 
    w2 = Window().partitionBy(['metadataUid', 'key', 'prefix', 'timestamp']).orderBy(['tmin', 'tmax'])
    s4 = s3.filter(col('timestamp') >= col('tmin'))\
            .withColumn('hasNext', (col('uid5') == lead('uid5').over(w2)) & (col('tmin') < lead('tmin').over(w2)))
    
    # find the smallest tmin which is larger than timestamp (nearest-future)
    # some records can both in s4 and s5, which have both timestamp >= tmin and timestamp < tmin
    s5 = s3.filter(col('timestamp') < col('tmin'))\
            .withColumn('hasPrev', (col('uid5') == lag('uid5').over(w2)) & (col('tmin') >= lag('tmin').over(w2)))
   
    # we prefer nearest-history(hasNext=null) over nearest-future(hasPrev=null) 
    s4x = s4.filter(col('hasNext').isNull()).withColumn('matchFrom', lit('hasNext')).drop('hasNext')
    s5x = s5.filter(col('hasPrev').isNull()).withColumn('matchFrom', lit('hasPrev')).drop('hasPrev')
    s6 = s4x.union(s5x)\
            .withColumn('hasLeft', (col('uid5') == lag('uid5').over(w2)) & (col('tmin') >= lag('tmin').over(w2)))
    
    # vagueMatch has 1738377 rows
    vagueMatch = s6.filter(col('hasLeft').isNull())\
                    .select(resKeyList)
    
    # we can view vagueMatchFutureOnly as not matched if necessary 
    '''
    # vagueMatchHistory has 1738222 rows, vagueMatchFutureOnly has 152 rows
    vagueMatchHistory = s7.filter(col('hasLeft').isNull() & (col('matchFrom') == 'hasNext'))\
                        .select(resKeyList)
    vagueMatchFutureOnly = s7.filter(col('hasLeft').isNull() & (col('matchFrom') == 'hasPrev'))\
                            .select(resKeyList)
    '''
    # another easy understanding method: 
    # we can also firstly match [uid5, timestamp] with the nearest-history uid2, then join back
    # we confirmed that vagueMatch == vagueMatchy
    # Note: hasNext can only be True or null, and False is not allowed. lead() is the subsequent row
    # if the hasNext.isNull is kept the last one, the hasNext=False rows will not affect 
    '''
    yy = s3.select('uid5', 'timestamp', 'uid2', 'tmin', 'tmax').distinct() 
    w1 = Window().partitionBy(['uid5', 'timestamp']).orderBy(['tmin', 'tmax'])
    yy2 = yy.filter(col('timestamp') >= col('tmin'))\
            .withColumn('hasNext', (col('uid5') == lead('uid5').over(w1)) & (col('tmin') < lead('tmin').over(w1)))
    yy3 = yy2.filter(col('hasNext').isNull())
    vagueMatchy = s3.join(yy3, yy.schema.names, 'inner')\
                    .select(resKeyList)
    '''

    # if not matched, possibly wrong ('User' with other namespace)
    # we have checked that metaNamespace1 will not introduce False Positive, and may introduce False Nagative
    # but currently, we compare the result with two-step-join method, the finally not matched are identical
    # therefore, we don't see any FN now, either
    # vagueNotMatch has 3340 rows
    vagueNotMatch = sx2.filter(col('uid2').isNull())\
                        .withColumn('name2', col('name1'))\
                        .withColumn('namespace2', col('namespace1'))\
                        .withColumn('kind2', col('kind1'))\
                        .select(resKeyList)
    
    # return the union
    return  metaEntity.union(clearEntityRes)\
                    .union(vagueMatch)\
                    .union(vagueNotMatch)\
