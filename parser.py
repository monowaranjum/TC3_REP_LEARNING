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
    parent_subject = json_subject['parentSubject']
    host_id = json_subject['hostId']
    local_prinicpal = json_subject['localPrincipal']
    start_time_stamp_nanos = json_subject['startTimeStampNanos']
    unit_id = json_subject['unitId']
    iteration = json_subject['iteration']
    count = json_subject['count']
    cmd_line = json_subject['cmdLine']
    privelege_level = json_subject['privelegeLevel']
    imported_libraries = json_subject['importedLibraries']
    exported_libraries = json_subject['exportedLibraries']

    subject = orm.Subject(uuid, type, cid, parent_subject, host_id, local_prinicpal, start_time_stamp_nanos, unit_id, iteration, count, cmd_line, privelege_level, imported_libraries, exported_libraries)

    return subject


def parse_event(json_event):
    '''
    This function will parse the JSON object 
    and populate the Event ORM object.
    '''
    uuid = json_event['uuid']
    sequence = json_event['sequence']["long"]
    type = json_event['type']
    thread_id = json_event['threadId']["int"]
    host_id = json_event['hostId']
    subject = json_event['subject']["com.bbn.tc.schema.avro.cdm18.UUID"]
    predicate_object = json_event['predicateObject']
    predicate_object_path = json_event['predicateObjectPath']
    predicate_object_2 = json_event['predicateObject2']
    predicate_object_path_2 = json_event['predicateObjectPath2']
    time_stamp_nanos = json_event['timeStampNanos']
    name = json_event['name']['string']
    location = json_event['location']
    size = json_event['size']
    program_point = json_event['programPoint']

    event = orm.Event(uuid, sequence, type, thread_id, host_id, subject, predicate_object, predicate_object_path, predicate_object_2, predicate_object_path_2, time_stamp_nanos, name, location, size, program_point)

    return event

def parse_file_object(json_file_object):
    '''
    This function will parse the JSON object 
    and populate the File ORM object.
    '''
    uuid = json_file_object['uuid']
    base_object_host_id = json_file_object['baseObject']['hostId']
    base_object_permission = json_file_object['baseObject']['permission']
    type = json_file_object['type']
    file_descriptor = json_file_object['fileDescriptor']
    local_principal = json_file_object['localPrincipal']
    size = json_file_object['size']

    file_object = orm.FileObject(uuid, base_object_host_id, base_object_permission, type, file_descriptor, local_principal, size)

    return file_object


def unnamed_pipe_object(json_unnamed_pipe_object):
    '''
    This function will parse the JSON object 
    and populate the UnnamedPipeObject ORM object.
    '''
    uuid = json_unnamed_pipe_object['uuid']
    base_object_host_id = json_unnamed_pipe_object['baseObject']['hostId']
    base_object_permission = json_unnamed_pipe_object['baseObject']['permission']
    source_file_descriptor = json_unnamed_pipe_object['sourceFileDescriptor']
    sink_file_descriptor = json_unnamed_pipe_object['sinkFileDescriptor']
    source_uuid = json_unnamed_pipe_object['sourceUuid']
    sink_uuid = json_unnamed_pipe_object['sinkUuid']

    unnamed_pipe_object = orm.UnnamedPipeObject(uuid, base_object_host_id, base_object_permission, source_file_descriptor, sink_file_descriptor, source_uuid, sink_uuid)

    return unnamed_pipe_object


def registry_key_object(json_registry_key_object):
    '''
    This function will parse the JSON object 
    and populate the RegistryKeyObject ORM object.
    '''
    uuid = json_registry_key_object['uuid']
    base_object_host_id = json_registry_key_object['baseObject']['hostId']
    base_object_permission = json_registry_key_object['baseObject']['permission']
    key = json_registry_key_object['key']

    registry_key_object = orm.RegistryKeyObject(uuid, base_object_host_id, base_object_permission, key)

    return registry_key_object

def memory_object(json_memory_object):
    '''
    This function will parse the JSON object 
    and populate the MemoryObject ORM object.
    '''
    uuid = json_memory_object['uuid']
    base_object_host_id = json_memory_object['baseObject']['hostId']
    base_object_permission = json_memory_object['baseObject']['permission']
    memory_address = json_memory_object['memoryAddress']
    page_number = json_memory_object['pageNumber']
    page_offset = json_memory_object['pageOffset']
    size = json_memory_object['size']
    
    memory_object = orm.MemoryObject(uuid, base_object_host_id, base_object_permission, memory_address, page_number, page_offset, size)

    return memory_object

def net_flow_object(json_net_flow_object):
    '''
    This function will parse the JSON object 
    and populate the NetFlowObject ORM object.
    '''
    uuid = json_net_flow_object['uuid']
    base_object_host_id = json_net_flow_object['baseObject']['hostId']
    base_object_permission = json_net_flow_object['baseObject']['permission']
    local_address = json_net_flow_object['localAddress']
    local_port = json_net_flow_object['localPort']
    remote_address = json_net_flow_object['remoteAddress']
    remote_port = json_net_flow_object['remotePort']
    ip_protocol = json_net_flow_object['ipProtocol']
    file_descriptor = json_net_flow_object['fileDescriptor']
    
    net_flow_object = orm.NetFlowObject(uuid, base_object_host_id, base_object_permission, local_address, local_port, remote_address, remote_port, ip_protocol, file_descriptor)

    return net_flow_object

def src_sink_object(json_src_sink_object):
    '''
    This function will parse the JSON object 
    and populate the SrcSinkObject ORM object.
    '''
    uuid = json_src_sink_object['uuid']
    base_object_host_id = json_src_sink_object['baseObject']['hostId']
    base_object_permission = json_src_sink_object['baseObject']['permission']
    type = json_src_sink_object['type']
    file_descriptor = json_src_sink_object['fileDescriptor']

    src_sink_object = orm.SrcSinkObject(uuid, base_object_host_id, base_object_permission, type, file_descriptor)

    return src_sink_object

def packet_socket_object(json_packet_socket_object):
    '''
    This function will parse the JSON object 
    and populate the PacketSocketObject ORM object.
    '''
    uuid = json_packet_socket_object['uuid']
    base_object_host_id = json_packet_socket_object['baseObject']['hostId']
    base_object_permission = json_packet_socket_object['baseObject']['permission']
    proto = json_packet_socket_object['proto']
    if_index = json_packet_socket_object['ifIndex']
    ha_type = json_packet_socket_object['haType']
    pkt_type = json_packet_socket_object['pktType']
    addr = json_packet_socket_object['addr']

    packet_socket_object = orm.PacketSocketObject(uuid, base_object_host_id, base_object_permission, proto, if_index, ha_type, pkt_type, addr)

    return packet_socket_object

def host_object(json_host_object):
    '''
    This function will parse the JSON object 
    and populate the HostObject ORM object.
    '''
    uuid = json_host_object['uuid']
    host_id = json_host_object['hostId']
    host_name = json_host_object['hostName']
    os_details = json_host_object['osDetails']
    host_type = json_host_object['hostType']

    host_object = orm.Host(uuid, host_id, host_name, os_details, host_type)

    return host_object


def principal_object(json_principal_object):
    '''
    This function will parse the JSON object 
    and populate the Principal ORM object.
    '''
    uuid = json_principal_object['uuid']
    type = json_principal_object['type']
    host_id = json_principal_object['hostId']
    user_id = json_principal_object['userId']
    username = json_principal_object['username']
    
    principal_object = orm.Principal(uuid, type, host_id, user_id, username)

    return principal_object


def provenance_tag_node_object(json_provenance_tag_node_object):
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