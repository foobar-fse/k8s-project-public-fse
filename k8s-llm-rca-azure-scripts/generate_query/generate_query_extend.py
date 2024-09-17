#!/usr/bin/env python

#from common.openai_generic_assistant import OpenAIGenericAssistant
from common.azure_openai_generic_assistant import OpenAIGenericAssistant

from datetime import datetime

def date_suffix():
    now = datetime.now()
    suffix = now.strftime("-%m%d-%H%M")
    return suffix

def setup_cypher_generator():
    instructions = "You are an expert in neo4j and cypher query language."
    name = "cypher-query-generator" + date_suffix()

    cypherQueryGenerator = OpenAIGenericAssistant()
    #cypherQueryGenerator.create_assistant(instructions, name, 'gpt-4o')
    # use azure gpt-4o deployment
    cypherQueryGenerator.create_assistant(instructions, name, 'foobar-gpt-4o') # replace with your deployment in azure
    cypherQueryGenerator.create_thread()


    print(name)
    print(cypherQueryGenerator.assistant.id)
    print(cypherQueryGenerator.thread.id)
    
    #print(f'https://platform.openai.com/playground?assistant={cypherQueryGenerator.assistant.id}&thread={cypherQueryGenerator.thread.id}')
    
    # for gpt-4o
    #https_prefix = 'https://platform.openai.com/playground/assistants'
    #https_prefix = 'https://oai.azure.com/portal/<uuid>/assistants'
    https_prefix = '' # replace with your azure url

    print(f'{https_prefix}?assistant={cypherQueryGenerator.assistant.id}&thread={cypherQueryGenerator.thread.id}')

    label_message = "Let's label the following prompt template as generation-template-1, and use it to generate cypher query later"
    cypherQueryGenerator.add_message(label_message)

    generation_template = build_generation_template()
    cypherQueryGenerator.add_message(generation_template)
    
    return cypherQueryGenerator


def extend_metapath_construct_string(partial_path):
    nodes = partial_path.nodes
    relationships = partial_path.relationships
    srcKind = nodes[0]['kind']
    rels_str = f"""
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, {srcKind}, involvedObject_uid;
    """
    for rel in relationships:
        rel_str = (', ').join([rel.type, rel['srcKind'], rel['destKind'], rel['key']])
        rels_str += rel_str + ';\n'
    return rels_str

# Note: metapath is a string here
def generate_cypher_query(metapath_str, error_message, namespace, timestamp, uuid, cypherQueryGenerator):
    # build the prompt 
    prompt = f"""
    Let's use generation-template-1 and generate a cypher query for the following example. Strictly follow the (srcKind)-[rel]->(destkind) ordering, don't reverse it. Use double-quotes ("") to enclose error message if it has any single-quote (') character, otherwise use single-quotes('') to enclos. Return only the generated Cypher query within a code block, formatted as below:
    ```cypher
    <cypher_query>
    ```
    Do not include any extraneous strings like 'generated_cypher_query'. 
    Ensure the first line inside the code block is the starting line of the Cypher query.

    the provided metapath is:
    {metapath_str}
    the error message to filtering is:
    {error_message}
    the namespace to filtering is:
    {namespace}
    the time to filtering is:
    {timestamp}
    the optional uuid to filtering is:
    {uuid}
    """
    cypherQueryGenerator.add_message(prompt)

    print('run assistant')
    cypherQueryGenerator.run_assistant()
    messages = cypherQueryGenerator.wait_get_last_k_message(1)
    cypher_query = extract_cypher(messages.data[0].content[0].text.value)
    
    print(f'the generated cypher query is :\n {cypher_query}\n')

    return cypher_query

def extract_cypher(message_str):
    cypher_part = message_str.split('```cypher')[1].split('```')[0].strip()
    return cypher_part


def run_and_filter_query(query_executor, cypher_query):
    # the records may contains dest nodes that not mentioned by the EVENT
    records = query_executor.run_query(cypher_query)

    # if only one record found, we keep it, otherwise, we check the compatibility 
    res = []
    if len(records) == 1:
        res = records
    else:
        # strictly compatible
        for record in records:
            if message_compatible_strict(record):
                res.append(record)
        if len(res) == 0:
            # loosely compatible
            for record in records:
                if message_compatible_loose(record):
                    res.append(record)
        
    if len(res) == 0:
        print('Warning: ALL records are not message compatible')
    
    return res


def message_compatible_strict(record):
    # message
    for ele in record: 
        if ele['kind'] == 'Event':
            message = ele['message'] 
    # by default, dest is the last element
    dest = record[len(record)-1]
    # check if the name is in the message
    if dest['isNative'] == 'true':
        k1 = 'name2'
    elif dest['isAtomic'] == 'true':
        k1 = 'val'
    elif dest['tag'] in ['nfs', 'hostPath']:
        k1 = 'path'
    elif dest['tag'] == 'container':
        k1 = 'containerName'
    elif dest['tag'] == 'image':
        k1 = 'imageName'
    
    return (dest[k1] in message)

def message_compatible_loose(record):
    #message = record[2]['message']
    for ele in record:
        if ele['kind'] == 'Event':
            message = ele['message']
    # by default, dest is the last element
    dest = record[len(record)-1] 

    # check if the kind is in the message
    if dest['isNative'] == 'true':
        k2 = 'kind2'
    elif dest['isNative'] == 'false':
        k2 = 'tag'
    
    # we expect exactly match, don't compare with lower case.
    # otherwise, the kind checking is too loose
    return (dest[k2] in message)

'''
def message_compatible(record):
    #message = record[2]['message']
    for ele in record:
        if ele['kind'] == 'Event':
            message = ele['message'] 
    # by default, dest is the last element
    dest = record[len(record)-1]
    # check if the name is in the message
    if dest['isNative'] == 'true':
        k1 = 'name2'
    elif dest['isAtomic'] == 'true':
        k1 = 'val'
    elif dest['tag'] in ['nfs', 'hostPath']:
        k1 = 'path'
    elif dest['tag'] == 'container':
        k1 = 'containerName'
    elif dest['tag'] == 'image':
        k1 = 'imageName'
   
    # check if the kind is in the message
    if dest['isNative'] == 'true':
        k2 = 'kind2'
    elif dest['isNative'] == 'false':
        k2 = 'tag'

    # we expect exactly match, don't compare with lower case.
    # otherwise, the kind checking is too loose

    return (dest[k1] in message) or (dest[k2] in message)
'''   


# we find that if we only match the 'uuid' to determine the EVENT, the generated query runs slowly,
# so we put the 'uuid' as the last filter

def build_generation_template():
    template = """
    Cypher Query Generation Prompt Template
Use this template to construct a Cypher query that follows a specific metapath and filters 'EVENT' nodes based on the value of 'message', 'metadata', 'timestamp', 'nextTimestamp' and optional 'metahash' properties. the INPUT contains an 'error message' (for message filter), a 'namespace' (for metadata filter) and a 'time' (for timestamp and nextTimestamp filter), and an optional 'uuid' (for metahash filter). As an example, we'll use a case where a 'ConfigMap' is not found.

   1. Analyze the Metapath and Error Message:
        ○ Break down the metapath into its components, where each segment includes a relationship type (relType), source node type (srcKind), destination node type (destKind), and a characteristic value (propertyValue) associated with a consistently named property on the relationship. This property is uniformly named 'key' across relationships. To filter for a specific relationship, you reference this 'key' along with the provided characteristic value, as expressed in the pattern r.key = 'propertyValue'.
        ○ Identify the error message to be used for filtering, paying attention to its exact wording for string matching.

    2. Start with Filtering EVENT Nodes:
        (1) Begin by matching EVENT nodes that have a property named 'message'.
        ○ Use a WHERE clause with the CONTAINS function to tolerate variations like trailing spaces or word case in the message
        ○ Ensure the full error message is included in the query's WHERE clause against the 'message' property of the EVENT nodes, without truncation.
        ○ Ensure to encase the error message in double-quotes ("") if it includes a single-quote (') character, otherwise, opt for single-quotes ('') for encapsulation.
        MATCH (e:EVENT)
        WHERE e.message CONTAINS "Error message with single-quote's character" OR e.message CONTAINS 'Error message without single-quote'
        RETURN e

        (2) Continue by matching EVENT nodes that has a property named 'metadata'. 
        ○ Use AND clause with the CONTAINS function to filter the 'namespace' that contained in metadata.
        MATCH (e:EVENT)
        WHERE e.message CONTAINS "Error message with single-quote's character" OR e.message CONTAINS 'Error message without single-quote'
        AND e.metadata CONTAINS 'namespace'
        RETURN e

        (3) Futher by matching EVENT nodes that has a 'timestamp' and 'nextTimestamp' property.
        ○ Ensure that 'time' >= 'timestamp' and 'time' <= 'nextTimestamp', pay attention to the equality case.
        ○ Use a AND clause to follow the 'metadata' filter. 
        MATCH (e:EVENT)
        WHERE e.message CONTAINS "Error message with single-quote's character" OR e.message CONTAINS 'Error message without single-quote'
        AND e.metadata CONTAINS 'namespace'
        AND e.timestamp <= 'time'
        AND e.nextTimestamp >= 'time'
        RETURN e

        (4)If a 'uuid' is presented, further by matching EVENT nodes that has a property named 'metahash'.
        ○ Use a AND clause to follow the 'timestamp' and 'nextTimestamp' filter. 
        MATCH (e:EVENT)
        WHERE e.message CONTAINS "Error message with single-quote's character" OR e.message CONTAINS 'Error message without single-quote'
        AND e.metadata CONTAINS 'namespace'
        AND e.timestamp <= 'time'
        AND e.nextTimestamp >= 'time'
        AND e.metahash = 'uuid'
        RETURN e

        (5) Apply a LIMIT to narrow down the results early:
        MATCH (e:EVENT)
        WHERE e.message CONTAINS 'Your error message here'
        AND e.metadata CONTAINS 'namespace'
        AND e.timestamp <= 'time'
        AND e.nextTimestamp >= 'time'
        AND e.metahash = 'uuid'
        WITH e
        LIMIT 1


    3. Chain MATCH Clauses Based on the Metapath:
        ○ Continue the query by adding MATCH clauses for each part of the provided metapath. For each segment of the metapath, use the node type (srcKind and destKind) as the label for the source and destination node. Use the relationship type (relType) as the label for the connecting edge, and apply a WHERE clause based on the 'key' property value (propertyValue) specified for that relationship:
   
        MATCH (startNode:srcKind)-[r1:relType]->(node1:destKind)
        WHERE r1.key = 'propertyValue'
   
        ○ For consecutive relationships, increment the relationship alias sequentially to use unique identifiers such as r1, r2, r3, etc. This ensures clarity when multiple relationships are present in the MATCH pattern:

        MATCH (node1:srcKind)-[r2:relType]->(node2:destKind)
        WHERE r2.key = 'propertyValue'

        ... and so on for additional relationships.
        
        ○ Ensure to use the same node alias for each node type, particularly if that node type appears in multiple relationships to maintain consistency. For example:

        MATCH (evt: EVENT),
        MATCH (n1:Event)-[r1:HasState]->(evt: EVENT),
        MATCH (n1:Event)-[r2:ReferInternal]->(n2: Pod)

    4. Adhere Strictly to the Provided Labels and Property Values:
        ○ Use the node and relationship labels exactly as provided in the metapath without adjustments or reinterpretations.  For instance, if the label given is 'nfs', it should not be changed to 'NFS' or any other variation.
        ○ Ensure correct case sensitivity and spelling to match the labels in your Neo4j database exactly.
        ○ Use the property value exactly as provided in the metapath without adjustments or reinterpretations. For instance, if the property value is 'involvedObject_uid', don't omit the '_' or use other variation.

    5. Timely Filtering:
        ○ Apply the filters as soon as possible after each MATCH clause, rather than aggregating all filtering at the end of the query.
        ○ Timely filtering helps to reduce the search space and improve query performance.

    6. Construct the RETURN Statement:
        ○ Include all the matched nodes and relationships in the RETURN clause to generate the complete path as specified by the metapath:

RETURN startNode, rel, destNode, …

    7. Example Based on a ConfigMap Not Found Case:

    (1) If 'uuid' is not presented, for example

    Provided Metapath:
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, Pod, involvedObject_uid;
    ReferInternal, Pod, ConfigMap, spec_volumes_configMap_name

    Error Message for Filtering:
    MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found

    Namespace for Filtering:
    cuibo1

    time for Filtering:
    2020-12-12 08:35:02.012

    Generated Cypher Query:
    
    MATCH (evt:EVENT)
    WHERE evt.message CONTAINS 'MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found'
    AND evt.metadata CONTAINS 'cuibo1'
    AND evt.timestamp <= '2020-12-12 08:35:02.012'
    AND evt.nextTimestamp >= '2020-12-12 08:35:02.012'
    WITH evt
    LIMIT 1
    MATCH (event:Event)-[r1:HasEvent]->(evt)
    WHERE r1.key = 'metadata_uid'
    MATCH (event)-[r2:ReferInternal]->(pod:Pod)
    WHERE r2.key = 'involvedObject_uid'
    MATCH (pod)-[r3:ReferInternal]->(configMap:ConfigMap)
    WHERE r3.key = 'spec_volumes_configMap_name'
    RETURN event, r1, evt, r2, pod, r3, configMap

    (2) If the 'uuid' of Event is presented, for example:
    
    uuid for Filtering: 
    '76861283-b2b9-4f61-92e9-7feb6f62be2a'
   
    We can argument the Generated Cypher Query:

    MATCH (evt:EVENT)
    WHERE evt.message CONTAINS 'MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found'
    AND evt.metadata CONTAINS 'cuibo1'
    AND evt.timestamp <= '2020-12-12 08:35:02.012'
    AND evt.nextTimestamp >= '2020-12-12 08:35:02.012'
    AND evt.metahash = '76861283-b2b9-4f61-92e9-7feb6f62be2a'
    WITH evt
    LIMIT 1
    MATCH (event:Event)-[r1:HasEvent]->(evt)
    WHERE r1.key = 'metadata_uid'
    MATCH (event)-[r2:ReferInternal]->(pod:Pod)
    WHERE r2.key = 'involvedObject_uid'
    MATCH (pod)-[r3:ReferInternal]->(configMap:ConfigMap)
    WHERE r3.key = 'spec_volumes_configMap_name'
    RETURN event, r1, evt, r2, pod, r3, configMap

    """

    return template


def human_generate_cypher_query(metapath_str, error_message, namespace, timestamp, uuid=None):
    # Parse the metapath string into list
    mp = metapath_str.split(';')[:-1]
    metapath = [rel.strip().split(', ') for rel in mp]
    
    # Start with Filtering EVENT Nodes
    query_parts = []
    node_aliases = {"EVENT": "evt"}

    # we can not require that nextTimestamp > timestamp, if only one EVENT 
    query_parts.append(f"""
MATCH (evt:EVENT)
WHERE evt.message CONTAINS {repr(error_message)}
AND evt.metadata CONTAINS {repr(namespace)}
AND evt.timestamp <= {repr(timestamp)} 
AND evt.nextTimestamp >= {repr(timestamp)}""")

    if (uuid != None):
        query_parts.append(f"""
AND evt.metahash = {repr(uuid)}
WITH evt
LIMIT 1""")
    else:
        query_parts.append(f"""
WITH evt
LIMIT 1""")
        
    # Build node alias for each srcKind and destKind
    idx = 1
    for rel in metapath:
        srcKind = rel[1]
        destKind = rel[2]
        if srcKind not in node_aliases:
            node_aliases[srcKind] = f"n{idx}"
            idx += 1
        if destKind not in node_aliases:
            node_aliases[destKind] = f"n{idx}"
            idx += 1

    # Chain MATCH Clauses Based on the Metapath with Timely Filtering
    for idx, (relType, srcKind, destKind, propertyValue) in enumerate(metapath, start=1):
        srcAlias = node_aliases[srcKind]
        destAlias = node_aliases[destKind]

        query_parts.append(f"""
MATCH ({srcAlias}:{srcKind})-[r{idx}:{relType}]->({destAlias}:{destKind})
WHERE r{idx}.key = {repr(propertyValue)}""")

    # Construct the RETURN Statement
    nodes = list(node_aliases.values())
    rels = [f"r{idx}" for idx in range(1, len(metapath)+1)]
    assert len(nodes) == len(rels) + 1

    return_vars = [None]*(len(nodes)+len(rels))
    return_vars[::2] = nodes
    return_vars[1::2] = rels

    query_parts.append(f"""
RETURN {', '.join(return_vars)}""")

    # Combine all parts into a complete Cypher query
    complete_query = '\n'.join(query_parts) # or '\n'.join() with new line
    
    print(f'the human generated cypher query is: \n{complete_query}\n')
    return complete_query.strip()


