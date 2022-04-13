import os 
import json
from re import sub
from tkinter.messagebox import NO 
import db_classes as orm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2


graph = dict() # The graph object to hold the provenance graph
index = dict() # The index mapping for quick retrieval of objects

index_file_location = '/mnt/8tb/csenrc/representation_learning_codes/index_file.json'
graph_file_location = '/mnt/8tb/csenrc/representation_learning_codes/graph_file.json'

mapping_dict = {"EVENT":1, 
                "HOST":2, 
                "PRINCIPAL":3, 
                "SUBJECT":4, 
                "FILE":5,
                "UNNAMED_PIPE":6,
                "MEMORY":7,
                "NETFLOW":8,
                "SRC_SINK":9,
                "PACKET_SOCKET":10,
                "PROVENANCE_TAG":11,
                "REGISTRY_KEY":12,
                1:"EVENT", 
                2:"HOST", 
                3:"PRINCIPAL", 
                4:"SUBJECT", 
                5:"FILE",
                6:"UNNAMED_PIPE",
                7:"MEMORY",
                8:"NETFLOW",
                9:"SRC_SINK",
                10:"PACKET_SOCKET",
                11:"PROVENANCE_TAG",
                12:"REGISTRY_KEY"}


subject_types = { "SUBJECT_PROCESS": 1, "SUBJECT_THREAD": 2, "SUBJECT_UNIT": 3, "SUBJECT_BASIC_BLOCK": 4 }


event_types = { "EVENT_ACCEPT": 1, "EVENT_ADD_OBJECT_ATTRIBUTE": 2, "EVENT_BIND": 3, "EVENT_BLIND": 4, "EVENT_BOOT": 5, "EVENT_CHANGE_PRINCIPAL": 6, "EVENT_CHECK_FILE_ATTRIBUTES": 7, "EVENT_CLONE": 8, "EVENT_CLOSE": 9, "EVENT_CONNECT": 10, "EVENT_CREATE_OBJECT": 11, "EVENT_CREATE_THREAD": 12, "EVENT_DUP": 13, "EVENT_EXECUTE": 14, "EVENT_EXIT": 15, "EVENT_FLOWS_TO": 16, "EVENT_FCNTL": 17, "EVENT_FORK": 18, "EVENT_LINK": 19, "EVENT_LOADLIBRARY": 20, "EVENT_LOGCLEAR": 21, "EVENT_LOGIN": 22, "EVENT_LOGOUT": 23, "EVENT_LSEEK": 24, "EVENT_MMAP": 25, "EVENT_MODIFY_FILE_ATTRIBUTES": 26, "EVENT_MODIFY_PROCESS": 27, "EVENT_MOUNT": 28, "EVENT_MPROTECT": 29, "EVENT_OPEN": 30, "EVENT_OTHER": 31, "EVENT_READ": 32, "EVENT_READ_SOCKET_PARAMS": 33, "EVENT_RECVFROM": 34, "EVENT_RECVMSG": 35, "EVENT_RENAME": 36, "EVENT_SENDTO": 37, "EVENT_SENDMSG": 38, "EVENT_SERVICEINSTALL":39, "EVENT_SHM": 40, "EVENT_SIGNAL": 41, "EVENT_STARTSERVICE":42, "EVENT_TRUNCATE": 43, "EVENT_UMOUNT": 44, "EVENT_UNIT": 45, "EVENT_UNLINK": 46, "EVENT_UPDATE": 47, "EVENT_WAIT": 48, "EVENT_WRITE": 49, "EVENT_WRITE_SOCKET_PARAMS": 50 }

file_types = { "FILE_OBJECT_BLOCK": 1, 
                "FILE_OBJECT_CHAR":2, 
                "FILE_OBJECT_DIR": 3, 
                "FILE_OBJECT_FILE": 4, 
                "FILE_OBJECT_LINK": 5, 
                "FILE_OBJECT_NAMED_PIPE": 6, 
                "FILE_OBJECT_PEFILE": 7, 
                "FILE_OBJECT_UNIX_SOCKET": 8 }

name_types = {
                'aue_link': 1 ,
                'aue_pdfork': 2 ,
                'aue_setresgid': 3 ,
                'aue_chown': 4 ,
                'aue_connect': 5 ,
                'aue_rename': 6 ,
                'aue_ftruncate': 7 ,
                'aue_socketpair': 8 ,
                'aue_mmap': 9 ,
                'aue_pwrite': 10 ,
                'aue_utimes': 11 ,
                'aue_mkdirat': 12 ,
                'aue_closefrom': 13 ,
                'aue_exit': 14 ,
                'aue_sendto': 15 ,
                'aue_accept': 16 ,
                'aue_chmod': 17 ,
                'aue_unlinkat': 18 ,
                'aue_read': 19 ,
                'aue_recvfrom': 20 ,
                'aue_umask': 21 ,
                'aue_chdir': 22 ,
                'aue_fchown': 23 ,
                'aue_lseek': 24 ,
                'aue_futimes': 25 ,
                'aue_pipe': 26 ,
                'aue_kill': 27 ,
                'aue_fchmodat': 28 ,
                'aue_setgid': 29 ,
                'aue_close': 30 ,
                'aue_posix_openpt': 31 ,
                'aue_futimesat': 32 ,
                'aue_execve': 33 ,
                'aue_setuid': 34 ,
                'aue_mprotect': 35 ,
                'aue_setegid': 36 ,
                'aue_bind': 37 ,
                'aue_setlogin': 38 ,
                'aue_fcntl': 39 ,
                'aue_vfork': 40 ,
                'aue_rmdir': 41 ,
                'aue_listen': 42 ,
                'aue_writev': 43 ,
                'aue_open_rwtc': 44 ,
                'aue_recvmsg': 45 ,
                'aue_pread': 46 ,
                'aue_fchdir': 47 ,
                'aue_unlink': 48 ,
                'aue_mkdir': 49 ,
                'aue_setresuid': 50 ,
                'aue_setreuid': 51 ,
                'aue_sendmsg': 52 ,
                'aue_fchmod': 53 ,
                'aue_seteuid': 54 ,
                'aue_openat_rwtc': 55 ,
                'aue_write': 56 ,
                'aue_fork': 57 ,
            }


def get_node(session, uuid, table_code):
    table = mapping_dict[table_code]
    if table == "EVENT":
        return session.query(orm.Event).filter_by(uuid=uuid).first()
    elif table == "HOST":
        return session.query(orm.Host).filter_by(uuid=uuid).first()
    elif table == "PRINCIPAL":
        return session.query(orm.Principal).filter_by(uuid=uuid).first()
    elif table == "SUBJECT":
        return session.query(orm.Subject).filter_by(uuid=uuid).first()
    elif table == "FILE":
        return session.query(orm.FileObject).filter_by(uuid=uuid).first()
    elif table == "UNNAMED_PIPE":
        return session.query(orm.UnnamedPipeObject).filter_by(uuid=uuid).first()
    elif table == "MEMORY":
        return session.query(orm.MemoryObject).filter_by(uuid=uuid).first()
    elif table == "NETFLOW":
        return session.query(orm.NetFlowObject).filter_by(uuid=uuid).first()
    elif table == "SRC_SINK":
        return session.query(orm.SrcSinkObject).filter_by(uuid=uuid).first()
    elif table == "PACKET_SOCKET":
        return session.query(orm.PacketSocketObject).filter_by(uuid=uuid).first()
    elif table == "PROVENANCE_TAG":
        return session.query(orm.ProvenanceTagNode).filter_by(tag_id=uuid).first()
    elif table == "REGISTRY_KEY":
        return session.query(orm.RegistryKeyObject).filter_by(uuid=uuid).first()


def process_events(session, event_list):
    
    for event in event_list:
        
        try:

            subject_uuid = event.subject
            predicate_uuid_1 = event.predicate_object
            predicate_uuid_2 = event.predicate_object_2

            if subject_uuid is None:
                continue

            # Let's process the subject associated with the event.
            subject_node = session.query(orm.Subject).filter_by(uuid=subject_uuid).first()
            
            encoded_subject = None
            if subject_node is not None:
                encoded_subject = encode_subject(session, subject_node)

            if subject_node.id not in graph:
                graph[subject_node.id] = dict()
                graph[subject_node.id]['sub']= encoded_subject # Length 3
                graph[subject_node.id]['events'] = []

            # Now, the event itself needs to be processed.
            encoded_event = encode_event(event) # Length 5, total length with subject is 7 so far. 


            # Now, predicate object 1
            encoded_predicate_obj1 = None
            if predicate_uuid_1 is not None:
                temp_type = index[predicate_uuid_1]
                if temp_type == mapping_dict["FILE"]:
                    file_obj = get_node(session,predicate_uuid_1, temp_type)
                    encoded_predicate_obj1 = encode_file_object(file_obj, event.prediacte_object_path)
                elif temp_type == mapping_dict["NETFLOW"]:
                    nfl_obj = get_node(session,predicate_uuid_1, temp_type)
                    encoded_predicate_obj1 = encode_netflow_object(nfl_obj)

            #Now, predicate object 2
            encoded_predicate_obj2 = None
            if predicate_uuid_2 is not None:
                temp_type = index[predicate_uuid_2]
                if temp_type == mapping_dict["FILE"]:
                    file_obj = get_node(session,predicate_uuid_2, temp_type)
                    encoded_predicate_obj2 = encode_file_object(file_obj, event.predicate_object_path_2)
                elif temp_type == mapping_dict["NETFLOW"]:
                    nfl_obj = get_node(session,predicate_uuid_2, temp_type)
                    encoded_predicate_obj2 = encode_netflow_object(nfl_obj)

            graph[subject_node.id]['events'].append((encoded_event,encoded_predicate_obj1,encoded_predicate_obj2))

        except Exception as e:
            print(event)
            print(e)
            break





def encode_subject(session, subject_node):

    if subject_node is None: # Should not happen, just a sanity check
        return [None,None,None]

    # Feature 1: What kind of subject node is this
    sub_type = 0
    if subject_node.type is not None:
        sub_type = subject_types[subject_node.type]/len(subject_types) # Normalizing this data point
    # Feature 2: Who is the initiator of this subject (i.e., root, ordinary user, or unknown)
    principal = 0
    if subject_node.local_principal is not None:
        principal_node = get_node(session, subject_node.local_principal, mapping_dict['PRINCIPAL'])
        principal = principal_node.user_id
    # Feature 3: When did this subject came into being active (timestamp)?
    origin_time = 0
    if subject_node.start_time_stamp_nanos is not None:
        origin_time = subject_node.start_time_stamp_nanos
    # Return the features as a list
    return [sub_type, principal, origin_time] 
    

def encode_event(event_object):

    if event_object is None: # Well, this is a base case sanity check, this should not happen
        return [None,None,None,None]

    #Feature 1: Event Type
    event_type = 0
    if event_object.type is not None:
        event_type = event_types[event_object.type]/len(event_types)
    #Feature 2: Predicate object type of this event
    predicate_type = 0
    if event_object.predicate_object is not None:
        predicate_type = index[event_object.predicate_object]/12 # This is where we are finding out the predicate object type and then dividing it by number of possible objects which is 12
    # Feature 3: Predicate object 2 type of this event
    predicate_type2 = 0
    if event_object.predicate_object_2 is not None:
        predicate_type2 = index[event_object.predicate_object_2]/12 # Same as before
    # Feature 4: Event time stamp. This will be used to calculate the time delta. 
    time_stamp = 0
    if event_object.time_stamp_nanos is not None:
        time_stamp = event_object.time_stamp_nanos

    #Feature 5: Event name. This is different than event type and contains auxilary information regarding the nature of an event.
    event_name = 0
    if event_object.name is not None:
        event_name = name_types[event_object.name]/len(name_types)

    return [event_type, predicate_type, predicate_type2, time_stamp, event_name]

def encode_file_object(file_obj, path):
    # Well, this is where things start getting a bit complicated.
    # What to keep vs. what to discard is always a difficult thing.
    # For the current version we are keeping the file type and the access location type. 

    if file_obj is None:
        return [None, None]

    os_locations = ['bin','etc','lib','var','mnt','sbin','proc'] # Anything in this list gets 1
    user_locations = ['usr','tmp'] # Anything in this list gets 2, Anything outside these two lists gets -1 (UNKNOWN)
    type = 0
    if file_obj.type is not None:
        type = file_types[file_obj.type]/len(file_types) # Get type and then normalize
    file_location = 0
    if path is not None:
        path_components = path.split('/') # [ Split it like unix would do]
        if len(path_components)>=2:
            if path_components[1] in os_locations: # [ Ignore the first token and please don't ask why.]
                file_location = 1
            elif path_components[1] in user_locations: # [ Same]
                file_location = 2
            else:
                file_location = -1

    return [type, file_location]

def encode_netflow_object(netflow_obj):
    # To be frank, the ip addresses doesn't matter much. They are just numbers.
    # Port numbers don't matter much either. However, non-standard ports are sometimes marked as red flag.
    # Therefore, we are taking port numbers into account.
    # What matters is whether it is an inbound or an outbound trafiic and what protocol it is using.
    # Unfortunately,in CADETS dataset, ip_protocol is never present.
    # Moreover, this dataset happens to have a lot of non-standard port communications, so there's that. 
    return [netflow_obj.local_port/65535, netflow_obj.remote_port/65535] # normalization by division over possible number of ports.

def encode_src_sink_object(src_sink_oject):
    # This was a surprisingly weird revealation. 
    # From the doc: there are way too many src sink, but all for androids. 
    # Apparently, for UNIX systems there is only 1 source-sink object and that's IPC.
    # Which means the whole CADETS dataset has only a single value SRCSINK_IPC. 
    # This means, encoding this value is useless and therefore, ignpored. 
    # Oh, and file_descriptor does not exist for any of the 113350 entries. 
    # Therfore, I am not encoding that either. What to return then? A list with two None. 

    return [None, None]

if __name__ == "__main__":

    psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_tc3'
    psql_engine = create_engine(psql_connection_url)
    Session = sessionmaker(bind=psql_engine)
    session = Session()

    total_event_count = 41350895 # Ran a count on the event table
    batch_size = 500000
    event_list = []

    with open(index_file_location,'r') as index_file:
        index = json.load(index_file)

    index_file.close()

    print("INDEX LOADED.")

    for i in range(1,total_event_count,batch_size):
        start_index = i
        end_index = i+batch_size
        event_list = session.query(orm.Event).filter(orm.Event.id>=start_index, orm.Event.id<end_index)
        process_events(session, event_list)
        #event_list.clear()
        print('Cleared event from id={} to id = {}'.format(start_index,end_index))

    print("********************* PROCESSING COMPLETE ********************")

    with open(graph_file_location, 'w') as graph_file:
        json.dump(graph, graph_file)

    print("******************** GRAPH WRITING COMPLETE ********************")

    graph_file.close()
    session.close()






