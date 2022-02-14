import parser as ps
import db_classes as orm
import json
import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2

psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_tc3'

def bulk_dump_in_db(objects, hosts, principals, connection_string, batch_size = 100000):
    
    try:
        psql_engine = create_engine(connection_string)
        orm.BASE.metadata.create_all(psql_engine)
        Session = sessionmaker(bind=psql_engine)
        session = Session()

        # start_idx = 0
        # end_idx = batch_size
        # dump_count = 0
        # while end_idx < len(objects) :

        #     session.bulk_save_objects(objects[start_idx:end_idx])
        #     session.commit()
        #     start_idx = end_idx
        #     end_idx += batch_size
        #     print("Dump Iteration Complete {}".format(dump_count))
        #     dump_count += 1

        # end_idx = len(objects)
        
        # if start_idx < end_idx:
        #     session.bulk_save_objects(objects[start_idx:end_idx])
        #     session.commit()
        
        for host in hosts:
            q = session.query(type(host)).filter((host.__class__).uuid == host.uuid)
            if session.query(q.exists()).scalar():
                print("Host {} already exists in the database".format(host.uuid))
            else:
                session.add(host)
                session.commit()
                print("Host {} added to the database".format(host.uuid))

        for principal in principals:
            q = session.query(orm.Principal).filter(orm.Principal.uuid == principal.uuid)
            if session.query(q.exists()).scalar():
                print("Principal {} already exists in the database".format(principal.uuid))
            else:
                session.add(principal)
                print("Principal {} added to the database".format(principal.uuid))

        for object in object_holder:
            q = session.query(type(object)).filter((object.__class__).uuid == object.uuid)
            if session.query(q.exists()).scalar():
                print("Object {} already exists in the database".format(object.uuid))
                print(object)
                print("\n")
            else:
                session.add(object)
                print("Object {} added to the database".format(object.uuid))
        
        session.commit()

        session.close()

        
    
    except Exception as e:
        traceback.print_exc()
        session.close()
        return




data_files = [
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json'
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json.1'
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json.2'
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json'
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.1',
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.2',
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.3',
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.4',
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-2.json',
    # '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-2.json.1'
]


#data_file_name = '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-2.json.1'

object_holder = []
subject_count = 0
event_count = 0
file_count = 0
packet_socket_count = 0
registry_object_count = 0
unnamed_pipe_object_count =0
memory_object_count = 0
netflow_object_count = 0
src_sink_object_count = 0
host_count = 0
principal_count = 0
provenance_tag_node_count = 0

host_holder = []
principal_holder = []

for data_file_name in data_files:
    print("Parsing file: {}".format(data_file_name))
    with open(data_file_name,'r') as f:
        for line in f:
            try:
                db_object = json.loads(line)['datum']
                object_type = list(db_object.keys())[0]
                
                if object_type[29:] == 'Subject':
                    object_holder.append(ps.parse_subject(db_object[object_type]))
                    subject_count += 1
                elif object_type[29:] == 'Event':
                    object_holder.append(ps.parse_event(db_object[object_type]))
                    event_count += 1
                elif object_type[29:] == 'FileObject':
                    object_holder.append(ps.parse_file_object(db_object[object_type]))
                    file_count += 1
                elif object_type[29:] == 'PacketSocketObject':
                    object_holder.append(ps.parse_packet_socket_object(db_object[object_type]))
                    packet_socket_count += 1
                elif object_type[29:] == 'RegistryObject':
                    object_holder.append(ps.parse_registry_object(db_object[object_type]))
                    registry_object_count += 1
                elif object_type[29:] == 'UnnamedPipeObject':
                    object_holder.append(ps.parse_unnamed_pipe_object(db_object[object_type]))
                    unnamed_pipe_object_count += 1
                elif object_type[29:] == 'MemoryObject':
                    object_holder.append(ps.parse_memory_object(db_object[object_type]))
                    memory_object_count += 1
                elif object_type[29:] == 'NetFlowObject':
                    object_holder.append(ps.parse_netflow_object(db_object[object_type]))
                    netflow_object_count += 1
                elif object_type[29:] == 'SrcSinkObject':
                    object_holder.append(ps.parse_src_sink_object(db_object[object_type]))
                    src_sink_object_count += 1
                elif object_type[29:] == 'Host':
                    host_holder.append(ps.parse_host(db_object[object_type]))
                    host_count += 1
                elif object_type[29:] == 'Principal':
                    principal_holder.append(ps.parse_principal(db_object[object_type]))
                    principal_count += 1
                elif object_type[29:] == 'ProvenanceTagNode':
                    object_holder.append(ps.parse_provenance_tag_node(db_object[object_type]))
                    provenance_tag_node_count += 1
                else:
                    print(object_type[29:])

                if len(object_holder)>20:
                    break
            except Exception as e:
                traceback.print_exc()
                print("\n\n")
                print(db_object[object_type])
                #print("Error in parsing line: " + line)
                break

    bulk_dump_in_db(object_holder, host_holder, principal_holder, psql_connection_url, batch_size=100000)
    object_holder.clear()
    host_holder.clear()


print("Subject: {}".format(subject_count))
print("Event: {}".format(event_count))
print("File: {}".format(file_count))
print("Packet Socket: {}".format(packet_socket_count))
print("Registry: {}".format(registry_object_count))
print("Unnamed Pipe: {}".format(unnamed_pipe_object_count))
print("Memory: {}".format(memory_object_count))
print("Netflow: {}".format(netflow_object_count))
print("SrcSink: {}".format(src_sink_object_count))
print("Host: {}".format(host_count))
print("Principal: {}".format(principal_count))
print("Provenance Tag Node: {}".format(provenance_tag_node_count))





