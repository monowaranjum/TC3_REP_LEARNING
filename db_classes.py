from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BigInteger, Integer, Column, String

BASE = declarative_base()

class Subject(BASE):
    __tablename__ = 'Subject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256))
    type = Column(String(256)) #Keep [ "SUBJECT_PROCESS", "SUBJECT_THREAD", "SUBJECT_UNIT", "SUBJECT_BASIC_BLOCK" ]
    cid = Column(Integer) 
    parent_subject = Column(String(256)) # [Might be handy for downstream task]
    host_id = Column(String(256)) # [Don't keep for CADETS, keep if varies]
    local_principal = Column(String(256)) # [ Keep the user_id, it is unique for different user_names ]
    start_time_stamp_nanos = Column(BigInteger) # [ Keep ]
    unit_id = Column(Integer)
    iteration = Column(Integer)
    count = Column(Integer)
    cmd_line = Column(String(65535)) # [ Cadets doesn't have it, but if it exists, definitely keep (tokenization)]
    privilege_level = Column(String(256)) # [ Cadets doesn't have it, but if it exists, definitely keep]
    imported_libraries = Column(String(65535)) # [ Same as above ]
    exported_libraries = Column(String(65535)) # [ Same as above ]
    
    def __init__(self, uuid, type, cid, parent_subject, host_id, local_principal, start_time_stamp_nanos, unit_id, iteration, count, cmd_line, privilege_level, imported_libraries, exported_libraries):
        self.uuid = uuid
        self.type = type
        self.cid = cid
        self.parent_subject = parent_subject
        self.host_id = host_id
        self.local_principal = local_principal
        self.start_time_stamp_nanos = start_time_stamp_nanos
        self.unit_id = unit_id
        self.iteration = iteration
        self.count = count
        self.cmd_line = cmd_line
        self.privilege_level = privilege_level
        self.imported_libraries = imported_libraries
        self.exported_libraries = exported_libraries

    def __str__(self):
        return 'Subject(uuid={}, type={}, cid={}, parent_subject={}, host_id={}, local_principal={}, start_time_stamp_nanos={}, unit_id={}, iteration={}, count={}, cmd_line={}, privilege_level={}, imported_libraries={}, exported_libraries={})'.format(self.uuid, self.type, self.cid, self.parent_subject, self.host_id, self.local_principal, self.start_time_stamp_nanos, self.unit_id, self.iteration, self.count, self.cmd_line, self.privilege_level, self.imported_libraries, self.exported_libraries)

class Event(BASE):
    __tablename__ = 'Event'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    sequence = Column(BigInteger)
    type = Column(String(256)) #[Keep it]
    thread_id = Column(Integer) 
    host_id = Column(String(256)) # [ All same for CADETS]
    subject = Column(String(256)) # [ Map across the tables ]
    predicate_object = Column(String(256)) # [ same ]
    prediacte_object_path = Column(String(4095)) #[ same ]
    predicate_object_2 = Column(String(256)) # [ same ]
    predicate_object_path_2 = Column(String(4095)) # [ same ]
    time_stamp_nanos = Column(BigInteger) # [ Calculate the delta and encode it ]
    name = Column(String(256)) # [ Encode it ]
    location = Column(BigInteger) # [ Empty for CADETS ]
    size = Column(BigInteger) # [ Empty for CADETS ]
    program_point = Column(String(512)) # [ Empty for CADETS ]
    

    def __init__(self, uuid, sequence, type, thread_id, host_id, subject, predicate_object, prediacte_object_path, predicate_object_2, predicate_object_path_2, time_stamp_nanos, name, location, size, program_point):
        self.uuid = uuid
        self.sequence = sequence
        self.type = type
        self.thread_id = thread_id
        self.host_id = host_id
        self.subject = subject
        self.predicate_object = predicate_object
        self.prediacte_object_path = prediacte_object_path
        self.predicate_object_2 = predicate_object_2
        self.predicate_object_path_2 = predicate_object_path_2
        self.time_stamp_nanos = time_stamp_nanos
        self.name = name
        self.location = location
        self.size = size
        self.program_point = program_point

    def __str__(self):
        return 'Event(uuid={}, sequence={}, type={}, thread_id={}, host_id={}, subject={}, predicate_object={}, prediacte_object_path={}, predicate_object_2={}, predicate_object_path_2={}, time_stamp_nanos={}, name={}, location={}, size={}, program_point={})'.format(self.uuid, self.sequence, self.type, self.thread_id, self.host_id, self.subject, self.predicate_object, self.prediacte_object_path, self.predicate_object_2, self.predicate_object_path_2, self.time_stamp_nanos, self.name, self.location, self.size, self.program_point)

class FileObject(BASE): # [ For CADETS only file type is relevant ]
    __tablename__ = 'FileObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    type = Column(String(256))
    file_descriptor = Column(Integer)
    local_principal = Column(String(256))
    size = Column(Integer)
    
    def __init__(self, uuid, base_object_host_id, base_object_permission, type, file_descriptor, local_principal, size):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.type = type
        self.file_descriptor = file_descriptor
        self.local_principal = local_principal
        self.size = size

    def __str__(self):
        return 'FileObject(uuid={}, base_object_host_id={}, base_object_permission={}, type={}, file_descriptor={}, local_principal={}, size={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.type, self.file_descriptor, self.local_principal, self.size)

class UnnamedPipeObject(BASE):
    __tablename__ = 'UnnamedPipeObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    source_file_descriptor = Column(Integer)
    sink_file_descriptor = Column(Integer)
    source_uuid = Column(String(256))
    sink_uuid = Column(String(256))

    def __init__(self, uuid, base_object_host_id, base_object_permission, source_file_descriptor, sink_file_descriptor, source_uuid, sink_uuid):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.source_file_descriptor = source_file_descriptor
        self.sink_file_descriptor = sink_file_descriptor
        self.source_uuid = source_uuid
        self.sink_uuid = sink_uuid

    def __str__(self):
        return 'UnnamedPipeObject(uuid={}, base_object_host_id={}, base_object_permission={}, source_file_descriptor={}, sink_file_descriptor={}, source_uuid={}, sink_uuid={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.source_file_descriptor, self.sink_file_descriptor, self.source_uuid, self.sink_uuid)
    
class MemoryObject(BASE):  # [There are no MemoryObject in CADETS dataset]
    __tablename__ = 'MemoryObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    memory_address = Column(BigInteger)
    page_number = Column(BigInteger)
    page_offset = Column(BigInteger)
    size = Column(BigInteger)

    def __init__(self, uuid, base_object_host_id, base_object_permission, memory_address, page_number, page_offset, size):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.memory_address = memory_address
        self.page_number = page_number
        self.page_offset = page_offset
        self.size = size

    def __str__(self):
        return 'MemoryObject(uuid={}, base_object_host_id={}, base_object_permission={}, memory_address={}, page_number={}, page_offset={}, size={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.memory_address, self.page_number, self.page_offset, self.size)

class NetFlowObject(BASE): 
    __tablename__ = 'NetflowObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    local_address = Column(String(256)) # [ Keep ]
    local_port = Column(Integer) # [Keep]
    remote_address = Column(String(256)) # [ Keep ]
    remote_port = Column(Integer)  # [Keep]
    ip_protocol = Column(Integer) # [ Keep if exists ]
    file_descriptor = Column(Integer)

    def __init__(self, uuid, base_object_host_id, base_object_permission, local_address, local_port, remote_address, remote_port, ip_protocol, file_descriptor):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.local_address = local_address
        self.local_port = local_port
        self.remote_address = remote_address
        self.remote_port = remote_port
        self.ip_protocol = ip_protocol
        self.file_descriptor = file_descriptor

    def __str__(self):
        return 'NetFlowObject(uuid={}, base_object_host_id={}, base_object_permission={}, local_address={}, local_port={}, remote_address={}, remote_port={}, ip_protocol={}, file_descriptor={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.local_address, self.local_port, self.remote_address, self.remote_port, self.ip_protocol, self.file_descriptor)

class SrcSinkObject(BASE): # CADETS have no useful informaton for this type
    __tablename__ = 'SrcSinkObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    type = Column(String(512))
    file_descriptor = Column(Integer)

    def __init__(self, uuid, base_object_host_id, base_object_permission, type, file_descriptor):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.type = type
        self.file_descriptor = file_descriptor

    def __str__(self):
        return 'SrcSinkObject(uuid={}, base_object_host_id={}, base_object_permission={}, type={}, file_descriptor={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.type, self.file_descriptor)

class PacketSocketObject(BASE): # NO instances in CADETS dataset
    __tablename__ = 'PacketSocketObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    proto = Column(Integer)
    if_index = Column(Integer)
    ha_type = Column(Integer)
    pkt_type = Column(Integer)
    addr = Column(String(256))

    def __init__(self, uuid, base_object_host_id, base_object_permission, proto, if_index, ha_type, pkt_type, addr):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.proto = proto
        self.if_index = if_index
        self.ha_type = ha_type
        self.pkt_type = pkt_type
        self.addr = addr

    def __str__(self):
        return 'PacketSocketObject(uuid={}, base_object_host_id={}, base_object_permission={}, proto={}, if_index={}, ha_type={}, pkt_type={}, addr={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.proto, self.if_index, self.ha_type, self.pkt_type, self.addr)

class Host(BASE): # Cadets have a single host rebooted 3 times. TBH, I thought there would be two hosts. Weird.
    __tablename__ = 'Host'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    host_name = Column(String(256))
    os_details = Column(String(256))
    host_type = Column(String(256))

    def __init__(self, uuid, host_name, os_details, host_type):
        self.uuid = uuid
        self.host_name = host_name
        self.os_details = os_details
        self.host_type = host_type

    def __str__(self):
        return 'Host(uuid={}, host_name={}, os_details={}, host_type={})'.format(self.uuid, self.host_name, self.os_details, self.host_type)

class Principal(BASE): # Principal == USER
    __tablename__ = 'Principal'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    type = Column(String(256))
    host_id = Column(String(256))
    user_id = Column(String(256))
    username = Column(String(256))
    
    def __init__(self, uuid, type, host_id, user_id, username):
        self.uuid = uuid
        self.type = type
        self.host_id = host_id
        self.user_id = user_id
        self.username = username

    def __str__(self):
        return 'Principal(uuid={}, type={}, host_id={}, user_id={}, username={})'.format(self.uuid, self.type, self.host_id, self.user_id, self.username)

class ProvenanceTagNode(BASE): # CADETS dont have any of this object 
    __tablename__ = 'ProvenanceTagNode'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(String(256), primary_key=True)
    flow_object = Column(String(256))
    host_id = Column(String(256))
    subject = Column(String(256))
    system_call = Column(String(512))
    program_point = Column(String(512))

    def __init__(self, tag_id, flow_object, host_id, subject, system_call, program_point):
        self.tag_id = tag_id
        self.flow_object = flow_object
        self.host_id = host_id
        self.subject = subject
        self.system_call = system_call
        self.program_point = program_point

    def __str__(self):
        return 'ProvenanceTagNode(tag_id={}, flow_object={}, host_id={}, subject={}, system_call={}, program_point={})'.format(self.tag_id, self.flow_object, self.host_id, self.subject, self.system_call, self.program_point)

class RegistryKeyObject(BASE):
    __tablename__ = 'RegistryKeyObject'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), primary_key=True)
    base_object_host_id = Column(String(256))
    base_object_permission = Column(Integer)
    key = Column(String(512))

    def __init__(self, uuid, base_object_host_id, base_object_permission, key):
        self.uuid = uuid
        self.base_object_host_id = base_object_host_id
        self.base_object_permission = base_object_permission
        self.key = key

    def __str__(self):
        return 'RegistryKeyObject(uuid={}, base_object_host_id={}, base_object_permission={}, key={})'.format(self.uuid, self.base_object_host_id, self.base_object_permission, self.key)


