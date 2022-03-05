import os 
import json
import db_classes as orm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2

psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_tc3'

filename = '/mnt/8tb/csenrc/representation_learning_codes/index_file.json'

index_map = dict()

psql_engine = create_engine(psql_connection_url)
Session = sessionmaker(bind=psql_engine)
session = Session()

mapping_code = {"EVENT":1, 
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
                "REGISTRY_KEY":12}



event_uuid_list =  session.query(orm.Event.uuid).all()

for event_uuid in event_uuid_list:
    index_map[event_uuid[0]] = mapping_code["EVENT"]

event_uuid_list.clear()
print("Event indexing complete")

host_uuid_list =  session.query(orm.Host.uuid).all()

for host_uuid in host_uuid_list:
    index_map[host_uuid[0]] = mapping_code["HOST"]

host_uuid_list.clear()
print("Host indexing complete")

principal_uuid_list =  session.query(orm.Principal.uuid).all()

for principal_uuid in principal_uuid_list:
    index_map[principal_uuid[0]] = mapping_code["PRINCIPAL"]

principal_uuid_list.clear()
print("Principal indexing complete")

subject_uuid_list =  session.query(orm.Subject.uuid).all()

for subject_uuid in subject_uuid_list:
    index_map[subject_uuid[0]] = mapping_code["SUBJECT"]

subject_uuid_list.clear()
print("Subject indexing complete")

file_uuid_list =  session.query(orm.FileObject.uuid).all()

for file_uuid in file_uuid_list:
    index_map[file_uuid[0]] = mapping_code["FILE"]

file_uuid_list.clear()
print("File indexing complete")

unnamed_pipe_uuid_list =  session.query(orm.UnnamedPipeObject.uuid).all()

for unnamed_pipe_uuid in unnamed_pipe_uuid_list:
    index_map[unnamed_pipe_uuid[0]] = mapping_code["UNNAMED_PIPE"]

unnamed_pipe_uuid_list.clear()
print("Unnamed pipe indexing complete")

memory_object_uuid_list =  session.query(orm.MemoryObject.uuid).all()

for memory_object_uuid in memory_object_uuid_list:
    index_map[memory_object_uuid[0]] = mapping_code["MEMORY"]

memory_object_uuid_list.clear()
print("Memory object indexing complete")

netflow_object_uuid_list =  session.query(orm.NetFlowObject.uuid).all()

for netflow_object_uuid in netflow_object_uuid_list:
    index_map[netflow_object_uuid[0]] = mapping_code["NETFLOW"]

netflow_object_uuid_list.clear()
print("Netflow object indexing complete")

src_sink_object_uuid_list =  session.query(orm.SrcSinkObject.uuid).all()

for src_sink_object_uuid in src_sink_object_uuid_list:
    index_map[src_sink_object_uuid[0]] = mapping_code["SRC_SINK"]

src_sink_object_uuid_list.clear()
print("SrcSink object indexing complete")

packet_socket_object_uuid_list =  session.query(orm.PacketSocketObject.uuid).all()

for packet_socket_object_uuid in packet_socket_object_uuid_list:
    index_map[packet_socket_object_uuid[0]] = mapping_code["PACKET_SOCKET"]

packet_socket_object_uuid_list.clear()
print("Packet socket object indexing complete")

provenance_tag_node_uuid_list =  session.query(orm.ProvenanceTagNode.tag_id).all()

for provenance_tag_node_uuid in provenance_tag_node_uuid_list:
    index_map[provenance_tag_node_uuid[0]] = mapping_code["PROVENANCE_TAG"]

provenance_tag_node_uuid_list.clear()
print("Provenance tag node indexing complete")

registry_key_uuid_list =  session.query(orm.RegistryKeyObject.uuid).all()

for registry_key_uuid in registry_key_uuid_list:
    index_map[registry_key_uuid[0]] = mapping_code["REGISTRY_KEY"]

registry_key_uuid_list.clear()
print("Registry key indexing complete")

session.close()

print("Length of index map: ", len(index_map))

with open(filename, 'w') as outfile:
    json.dump(index_map, outfile)

outfile.close()
print("Index file creation complete")


