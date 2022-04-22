'''
This file conatins all the data parsing functions. The name of the functions are self explanatory. 
The functions will take a JSON formatted object as input and will parse 
the data and populate an corrsponding ORM mapped object to dump in DB. 
'''

import db_classes as orm

def parse_subject(json_subject):
    '''
    This function will parse the JSON object 
    and populate the Subject ORM object.
    '''
    uuid = json_subject['uuid']
    type = json_subject['type']
    cid = json_subject['cid']
    
    parent_subject = None
    if json_subject['parentSubject'] is not None:
        parent_subject = json_subject['parentSubject']['com.bbn.tc.schema.avro.cdm18.UUID']
    
    host_id = json_subject['hostId']
    local_prinicpal = json_subject['localPrincipal']
    start_time_stamp_nanos = json_subject['startTimestampNanos']
    
    unit_id = None
    if json_subject['unitId'] is not None:
        unit_id = json_subject['unitId']['int']
    
    iteration = None
    if json_subject['iteration'] is not None:
        iteration = json_subject['iteration']['int']

    count = None
    if json_subject['count'] is not None:
        count = json_subject['count']['int']
    
    cmd_line = None
    if json_subject['cmdLine'] is not None:
        cmd_line = json_subject['cmdLine']['string']
    
    privilege_level = None
    if json_subject['privilegeLevel'] is not None:
        privilege_level = json_subject['privilegeLevel']

    imported_libraries = None
    if json_subject['importedLibraries'] is not None:
        imported_libraries = json_subject['importedLibraries'].tostring()

    exported_libraries = None
    if json_subject['exportedLibraries'] is not None:
        exported_libraries = json_subject['exportedLibraries'].tostring()

    subject = orm.Subject(uuid, type, cid, parent_subject, host_id, local_prinicpal, 
    start_time_stamp_nanos, unit_id, iteration, count, cmd_line, privilege_level, imported_libraries, exported_libraries)

    return subject


def parse_event(json_event):
    '''
    This function will parse the JSON object 
    and populate the Event ORM object.
    '''
    uuid = json_event['uuid']
    sequence = None
    if json_event['sequence'] is not None:
        sequence = json_event['sequence']["long"]
    type = json_event['type']
    thread_id = None
    if json_event['threadId'] is not None:
        thread_id = json_event['threadId']["int"]
    host_id = json_event['hostId']
    
    subject = None
    if json_event['subject'] is not None:
        subject = json_event['subject']["com.bbn.tc.schema.avro.cdm18.UUID"]
    
    predicate_object = None
    if json_event['predicateObject'] is not None:
        predicate_object = json_event['predicateObject']["com.bbn.tc.schema.avro.cdm18.UUID"]
    
    predicate_object_path = None
    if json_event['predicateObjectPath'] is not None:
        predicate_object_path = json_event['predicateObjectPath']['string']

    predicate_object_2 = None
    if json_event['predicateObject2'] is not None:
        predicate_object_2 = json_event['predicateObject2']["com.bbn.tc.schema.avro.cdm18.UUID"]
    
    predicate_object2_path = None
    if json_event['predicateObject2Path'] is not None:
        predicate_object2_path = json_event['predicateObject2Path']['string']
    
    time_stamp_nanos = json_event['timestampNanos']
    
    name = None
    if json_event['name'] is not None:
        name = json_event['name']['string']

    location = None
    if json_event['location'] is not None:
        location = json_event['location']['long']
    
    size = None
    if json_event['size'] is not None:
        size = json_event['size']['long']    
    
    program_point = None
    if json_event['programPoint'] is not None:
        program_point = json_event['programPoint']['string']
    
    event = orm.Event(uuid, sequence, type, thread_id, host_id, subject, predicate_object, predicate_object_path, predicate_object_2, predicate_object2_path, time_stamp_nanos, name, location, size, program_point)

    return event

def parse_file_object(json_file_object):
    '''
    This function will parse the JSON object 
    and populate the File ORM object.
    '''
    uuid = json_file_object['uuid']

    condition = lambda x: int(x, base=16) if x is not None else None 
    
    base_object_host_id = None
    base_object_permission = None

    if json_file_object['baseObject'] is not None:
        base_object_host_id = json_file_object['baseObject']['hostId']
        if json_file_object['baseObject']['permission'] is not None:
            base_object_permission = json_file_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']
    type = json_file_object['type']
    file_descriptor = None
    
    if json_file_object['fileDescriptor'] is not None:
        file_descriptor = json_file_object['fileDescriptor']['int']
    
    local_principal = None
    if json_file_object['localPrincipal'] is not None:
        local_principal = json_file_object['localPrincipal']['com.bbn.tc.schema.avro.cdm18.UUID']
    
    size = None
    if json_file_object['size'] is not None:
        size = json_file_object['size']['long']

    file_object = orm.FileObject(uuid, base_object_host_id, condition(base_object_permission) , type, file_descriptor, local_principal, size)

    return file_object


def parse_unnamed_pipe_object(json_unnamed_pipe_object):
    '''
    This function will parse the JSON object 
    and populate the UnnamedPipeObject ORM object.
    '''
    uuid = json_unnamed_pipe_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None

    base_object_host_id = None
    base_object_permission = None
    if json_unnamed_pipe_object['baseObject'] is not None:
        base_object_host_id = json_unnamed_pipe_object['baseObject']['hostId']
        if json_unnamed_pipe_object['baseObject']['permission'] is not None:
            base_object_permission = json_unnamed_pipe_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']

    source_file_descriptor = None
    if json_unnamed_pipe_object['sourceFileDescriptor'] is not None:
        source_file_descriptor = json_unnamed_pipe_object['sourceFileDescriptor']['int']

    sink_file_descriptor = None
    if json_unnamed_pipe_object['sinkFileDescriptor'] is not None:
        sink_file_descriptor = json_unnamed_pipe_object['sinkFileDescriptor']['int']
    
    source_uuid = None
    if json_unnamed_pipe_object['sourceUUID'] is not None:
        source_uuid = json_unnamed_pipe_object['sourceUUID']['com.bbn.tc.schema.avro.cdm18.UUID']
    sink_uuid = None
    if json_unnamed_pipe_object['sinkUUID'] is not None:
        sink_uuid = json_unnamed_pipe_object['sinkUUID']['com.bbn.tc.schema.avro.cdm18.UUID']

    unnamed_pipe_object = orm.UnnamedPipeObject(uuid, base_object_host_id, condition(base_object_permission), source_file_descriptor, sink_file_descriptor, source_uuid, sink_uuid)

    return unnamed_pipe_object


def parse_registry_key_object(json_registry_key_object):
    '''
    This function will parse the JSON object 
    and populate the RegistryKeyObject ORM object.
    '''
    uuid = json_registry_key_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None

    base_object_host_id = None
    base_object_permission = None
    if json_registry_key_object['baseObject'] is not None:
        base_object_host_id = json_registry_key_object['baseObject']['hostId']
        if json_registry_key_object['baseObject']['permission'] is not None:
            base_object_permission = json_registry_key_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']

    key = json_registry_key_object['key']

    registry_key_object = orm.RegistryKeyObject(uuid, base_object_host_id, condition(base_object_permission), key)

    return registry_key_object

def parse_memory_object(json_memory_object):
    '''
    This function will parse the JSON object 
    and populate the MemoryObject ORM object.
    '''
    uuid = json_memory_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None

    base_object_host_id = None
    base_object_permission = None
    if json_memory_object['baseObject'] is not None:
        base_object_host_id = json_memory_object['baseObject']['hostId']
        if json_memory_object['baseObject']['permission'] is not None:
            base_object_permission = json_memory_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']

    memory_address = json_memory_object['memoryAddress']
    page_number = None
    if json_memory_object['pageNumber'] is not None:
        page_number = json_memory_object['pageNumber']['long']
    page_offset = None
    if json_memory_object['pageOffset'] is not None:
        page_offset = json_memory_object['pageOffset']['long']
    size = None
    if json_memory_object['size'] is not None:
        size = json_memory_object['size']['long']
    
    memory_object = orm.MemoryObject(uuid, base_object_host_id, condition(base_object_permission), memory_address, page_number, page_offset, size)

    return memory_object

def parse_netflow_object(json_net_flow_object):
    '''
    This function will parse the JSON object 
    and populate the NetFlowObject ORM object.
    '''
    uuid = json_net_flow_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None
    base_object_host_id = None
    base_object_permission = None
    
    if json_net_flow_object['baseObject'] is not None:
        base_object_host_id = json_net_flow_object['baseObject']['hostId']
        if json_net_flow_object['baseObject']['permission'] is not None:
            base_object_permission = json_net_flow_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']

    local_address = json_net_flow_object['localAddress']
    local_port = json_net_flow_object['localPort']
    remote_address = json_net_flow_object['remoteAddress']
    remote_port = json_net_flow_object['remotePort']
    ip_protocol = None
    if json_net_flow_object['ipProtocol'] is not None:
        ip_protocol = json_net_flow_object['ipProtocol']['int']
    
    file_descriptor = None
    if json_net_flow_object['fileDescriptor'] is not None:
        file_descriptor = json_net_flow_object['fileDescriptor']['int']
    
    net_flow_object = orm.NetFlowObject(uuid, base_object_host_id, condition(base_object_permission), local_address, local_port, remote_address, remote_port, ip_protocol, file_descriptor)

    return net_flow_object

def parse_src_sink_object(json_src_sink_object):
    '''
    This function will parse the JSON object 
    and populate the SrcSinkObject ORM object.
    '''
    uuid = json_src_sink_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None
    base_object_host_id = None
    base_object_permission = None
    
    if json_src_sink_object['baseObject'] is not None:
        base_object_host_id = json_src_sink_object['baseObject']['hostId']
        if json_src_sink_object['baseObject']['permission'] is not None:
            base_object_permission = json_src_sink_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']
    
    type = json_src_sink_object['type']
    file_descriptor = None
    if json_src_sink_object['fileDescriptor'] is not None:
        file_descriptor = json_src_sink_object['fileDescriptor']['int']

    src_sink_object = orm.SrcSinkObject(uuid, base_object_host_id, condition(base_object_permission), type, file_descriptor)

    return src_sink_object

def parse_packet_socket_object(json_packet_socket_object):
    '''
    This function will parse the JSON object 
    and populate the PacketSocketObject ORM object.
    '''
    uuid = json_packet_socket_object['uuid']
    condition = lambda x: int(x, base=16) if x is not None else None
    base_object_host_id = None
    base_object_permission = None
    
    if json_packet_socket_object['baseObject'] is not None:
        base_object_host_id = json_packet_socket_object['baseObject']['hostId']
        if json_packet_socket_object['baseObject']['permission'] is not None:
            base_object_permission = json_packet_socket_object['baseObject']['permission']['com.bbn.tc.schema.avro.cdm18.SHORT']
    
    proto = json_packet_socket_object['proto']
    if_index = json_packet_socket_object['ifIndex']
    ha_type = json_packet_socket_object['haType']
    pkt_type = json_packet_socket_object['pktType']
    addr = json_packet_socket_object['addr']

    packet_socket_object = orm.PacketSocketObject(uuid, base_object_host_id, condition(base_object_permission), proto, if_index, ha_type, pkt_type, addr)

    return packet_socket_object

def parse_host(json_host_object):
    '''
    This function will parse the JSON object 
    and populate the HostObject ORM object.
    '''
    uuid = json_host_object['uuid']
    host_name = json_host_object['hostName']
    os_details = json_host_object['osDetails']
    host_type = json_host_object['hostType']

    host_object = orm.Host(uuid, host_name, os_details, host_type)

    return host_object


def parse_principal(json_principal_object):
    '''
    This function will parse the JSON object 
    and populate the Principal ORM object.
    '''
    uuid = json_principal_object['uuid']
    type = json_principal_object['type']
    host_id = json_principal_object['hostId']
    user_id = json_principal_object['userId']
    username = None
    if json_principal_object['username'] is not None:
        username = json_principal_object['username']['string']
    
    
    principal_object = orm.Principal(uuid, type, host_id, user_id, username)

    return principal_object


def parse_provenance_tag_node(json_provenance_tag_node_object):
    '''
    This function will parse the JSON object 
    and populate the ProvenanceTagNode ORM object.
    '''
    tag_id = json_provenance_tag_node_object['tagId']
    flow_object = json_provenance_tag_node_object['flowObject']
    host_id = json_provenance_tag_node_object['hostId']
    subject = json_provenance_tag_node_object['subject']
    system_call = json_provenance_tag_node_object['systemCall']
    program_point =   json_provenance_tag_node_object['programPoint']
    
    provenance_tag_node_object = orm.ProvenanceTagNode(tag_id, flow_object, host_id, subject, system_call, program_point)

    return provenance_tag_node_object