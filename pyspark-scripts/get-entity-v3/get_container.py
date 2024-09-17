# get container entity from k8s api resource
# note: we only keep a single for [containerName, containerID] pair
# i.e, [status_containerStatuses_name, status_containerStatuses_containerID] only keep the former key

from pyspark.sql.functions import *

#containerTag = spark.read.csv(testPath + '/container.csv', header=True)
#containerResc = rescFlatDF3.join(containerTag, ['kind', 'key'], 'inner')

def get_container(containerResc):
    # filter out containerName and containerID
    containerName = containerResc.filter(col('tag2') == 'name').withColumnRenamed('val', 'containerName')
    containerID = containerResc.filter(col('tag2') == 'containerID').withColumnRenamed('val', 'containerID')
    
    # only care about 4 fields, no distinct needed
    tmpName = containerName.select('metadataUid', 'timestamp', 'prefix', 'containerName')
    tmpID = containerID.select('metadataUid', 'timestamp', 'prefix', 'containerID')

    # full outer join on the same prefix, same metadataUid and same timestamp
    # the prefix ordering in different timestamp may be different
    # the containerID in different timestamp can be seen varying
    # count(containerOutJoin) == 10059
    containerOutJoin = tmpName.join(tmpID, ['metadataUid', 'timestamp', 'prefix'], 'outer').cache()

    # find out the strictly matched container name and ID
    # count(containerMatch) = 4037, count(containerNameNotMatch) = 4263, count(containerIDNotMatch) = 1759
    # [status_containerStatuses_name, status_containerStatuses_containerID]
    # [status_initContainerStatuses_name, status_initContainerStatuses_containerID]
    # we should keep only name or containerID, otherwise, there are two edges to the same entity
    containerMatch = containerOutJoin.filter(col('containerName').isNotNull() & col('containerID').isNotNull()).cache()
    # ['metadataUid', 'timestamp', 'prefix', 'containerName', 'containerID'], for later select
    matchKeyList = containerMatch.schema.names

    # find out the unmatched containerName and containerID (not have the same prefix, but can in the same metadataUid)
    containerNameNotMatch = containerOutJoin.filter(col('containerID').isNull())
    containerIDNotMatch = containerOutJoin.filter(col('containerName').isNull())

    # containerNameNotMatch inner join on ['metadataUid', 'timestamp', 'containerName'], not left join 
    # we don't further join on ['metadataUid', 'containerName'] or ['containerName'], 
    # which will pair a containerName with many containerIDs from different timestamps
    # count(containerNameMatch) = 4037, the rest 226 names are not worth a further two-step-join (i.e, in get_image.py) 
    # [spec_containers_name, spec_initContainers_name]
    containerNameMatch = containerNameNotMatch.select('metadataUid', 'timestamp', 'prefix', 'containerName')\
                        .join(containerMatch.withColumnRenamed('prefix', 'prefix2'), ['metadataUid', 'timestamp', 'containerName'], 'inner')\
                        .select(matchKeyList)

    # containerIDNotMatch inner join on ['containerID']
    # we have checked the containerID only has one containerName, so it is a good identifier
    # therefore, we can match containerID across metadataUid and timestamp
    # count(containerIDMatch) = 1484, the rest 275 IDs without name are still unique
    tmpIDName = containerMatch.select('containerID', 'containerName').distinct() 
    # [status_containerStatuses_state_terminated_containerID, status_initContainerStatuses_state_terminated_containerID,\
    # status_containerStatuses_lastState_terminated_containerID]
    containerIDMatch = containerIDNotMatch.select('metadataUid', 'timestamp', 'prefix', 'containerID')\
                        .join(tmpIDName, 'containerID', 'inner')\
                        .select(matchKeyList)
    
    # join with containerName and containerID respectively, 
    # don't join with containerResc directly, otherwise the containerName and containerID is null while val is not null
    keyList = [x for x in containerResc.schema.names if x != 'val']   
    keyList.extend(['containerName', 'containerID'])
  
    # note: containerMatch, containerNameMatch, containerIDMatch all have non-null containerName and containerID
    '''
    # split into 3 parts: containerMatch, containerNameNotMatch and containerIDNotMatch
    t1 = containerName.join(containerMatch, ['metadataUid', 'timestamp', 'prefix', 'containerName'], 'left').cache()
    s1 = t1.filter(col('containerID').isNotNull())\
            .select(keyList)
    t2 = t1.filter(col('containerID').isNull()) # equal to containerNameNotMatch
    s2 = t2.drop('containerID')\
            .join(containerNameMatch, ['metadataUid', 'timestamp', 'prefix', 'containerName'], 'left')\
            .select(keyList)
    t3 = containerID.join(containerMatch, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
                    .filter(col('containerName').isNull()) # equal to containerIDNotMatch
    s3 = t3.drop('containerName')\
            .join(containerIDMatch, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
            .select(keyList)
    '''
    # we prefer containerName, and will avoid twice selection in containerMatch
    containerNameResult = containerName.join(containerMatch.union(containerNameMatch),\
                                                ['metadataUid', 'timestamp', 'prefix', 'containerName'], 'left')\
                                        .select(keyList)

    # we exclude the part that have been picked by containerMatch 
    containerIDExclude = containerID.join(containerMatch, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
                                .filter(col('containerName').isNull())\
                                .drop('containerName')
    
    containerIDResult = containerIDExclude.join(containerIDMatch, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
                                    .select(keyList)
    '''
    # we use containerMatch and containerNameMatch to select xxx_name
    # and use containerIDMatch to select xx_id
    # we don't use containerMatch twice to both select xxx_name and xxx_id
    
    containerNameResult = containerName.join(containerMatch.union(containerNameMatch),\
                                                ['metadataUid', 'timestamp', 'prefix', 'containerName'], 'left')\
                                        .select(keyList)
    
    # the left join includes the unmatched IDs in containerIDMatch, and they can be viewed as entity too (without containerName)
    # bug: [status_containerStatuses_containerID, status_initContainerStatuses_containerID] in containerID will left
    # while, we have joined with containerMatch in [status_containerStatuses_name, status_initContainerStatuses_name]
    containerIDResult = containerID.join(containerIDMatch, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
                                    .select(keyList)
    '''
    '''
    # union together
    # warning: the name and containerID in containerMatch will be both matched, introducing double edges in later graph
    containerUnion = containerMatch.union(containerNameMatch).union(containerIDMatch)

    containerNameResult = containerName.join(containerUnion, ['metadataUid', 'timestamp', 'prefix', 'containerName'], 'left')\
                                .select(keyList)

    # the left join includes the unmatched IDs in containerIDMatch, and they can be viewed as entity too (without containerName)
    containerIDResult = containerID.join(containerUnion, ['metadataUid', 'timestamp', 'prefix', 'containerID'], 'left')\
                                .select(keyList)
    '''
    # return the union 
    return containerNameResult.union(containerIDResult)
