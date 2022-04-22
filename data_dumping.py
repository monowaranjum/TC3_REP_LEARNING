import parser as ps
import db_classes as orm
import json
import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
import multiprocessing as mp
import psutil
from datetime import datetime as dt

# psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_tc3'
# psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_theia'
# psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_trace'

def bulk_dump_in_db(objects, connection_string, batch_size = 100000):
    
    try:
        psql_engine = create_engine(connection_string)
        orm.BASE.metadata.create_all(psql_engine)
        Session = sessionmaker(bind=psql_engine)
        session = Session()

        start_idx = 0
        end_idx = batch_size
        dump_count = 0
        while end_idx < len(objects) :

            session.bulk_save_objects(objects[start_idx:end_idx])
            session.commit()
            start_idx = end_idx
            end_idx += batch_size
            print("Dump Iteration Complete {}".format(dump_count))
            dump_count += 1

        end_idx = len(objects)
        
        if start_idx < end_idx:
            session.bulk_save_objects(objects[start_idx:end_idx])
            session.commit()
        
        # for host in hosts:
        #     q = session.query(type(host)).filter((host.__class__).uuid == host.uuid)
        #     if session.query(q.exists()).scalar():
        #         print("Host {} already exists in the database".format(host.uuid))
        #     else:
        #         session.add(host)
        #         session.commit()
        #         print("Host {} added to the database".format(host.uuid))

        # for principal in principals:
        #     q = session.query(orm.Principal).filter(orm.Principal.uuid == principal.uuid)
        #     if session.query(q.exists()).scalar():
        #         print("Principal {} already exists in the database".format(principal.uuid))
        #     else:
        #         session.add(principal)
        #         print("Principal {} added to the database".format(principal.uuid))

        # for object in object_holder:
        #     q = session.query(type(object)).filter((object.__class__).uuid == object.uuid)
        #     if session.query(q.exists()).scalar():
        #         print("Object {} already exists in the database".format(object.uuid))
        #         print(object)
        #         print("\n")
        #     else:
        #         session.add(object)
        #         print("Object {} added to the database".format(object.uuid))
        
        # session.commit()

        session.close()

        
    
    except Exception as e:
        traceback.print_exc()
        session.close()
        return




data_files = [
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json.1',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official.json.2',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.1',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.2',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.3',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-1.json.4',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-2.json',
    '/mnt/8tb/csenrc/tc3/data/cadets/ta1-cadets-e3-official-2.json.1'
]

theia_data_files = [
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.1',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.1',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.10',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.2',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.11',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.3', 
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.12',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.4',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.2',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.5',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.3',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.6',  
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.4',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.7',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.5',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.8',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.6',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-1r.json.9',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.7',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-3.json',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.8',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-5m.json',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json.9',
    '/mnt/8tb/csenrc/tc3/data/theia/extracted_json/ta1-theia-e3-official-6r.json'
]

trace_data_files = [
    # '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.78',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.147',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.114',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.92',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.103',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.128',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.186',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.66',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.47',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.169',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.155',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.67',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.95',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.174',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.62',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.195',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.5',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.132',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.107',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.202',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.53',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.127',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.63',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.9',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.120',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.182',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.123',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.21',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.40',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.134',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.51',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.13',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.145',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.46',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.199',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.88',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.33',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.74',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.151',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.168',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.22',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.96',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.137',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.1',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.38',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.98',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.28',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.108',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.91',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.70',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.60',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.19',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.167',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.143',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.192',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.29',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.94',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.150',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.190',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.11',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.172',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.106',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.139',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.68',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.35',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.163',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.80',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.73',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.20',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.105',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.61',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.136',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.121',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.2',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.18',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.90',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.178',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.58',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.34',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.2',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.69',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.30',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.93',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.112',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.54',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.135',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.59',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.50',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.101',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.75',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.85',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.191',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.32',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.43',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.187',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.131',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.72',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.77',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.82',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.1',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.176',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.57',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.157',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.23',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.181',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.4',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.180',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.100',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.197',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.26',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.119',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.12',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.25',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.110',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.171',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.111',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.104',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.116',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.41',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.39',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.142',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.27',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.196',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.65',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.154',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.159',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.194',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.141',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.15',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.201',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.189',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.97',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.117',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.161',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.203',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.118',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.8',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.162',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.52',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.24',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.173',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.6',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.81',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.4',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.122',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.76',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.84',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.153',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.198',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.164',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.185',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.3',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.170',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.109',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.42',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.44',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.115',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.125',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.156',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.146',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.184',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.166',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.140',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.148',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.64',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.7',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.86',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.129',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.56',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.102',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.55',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.16',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.49',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.37',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.113',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.71',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.200',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.133',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.126',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.144',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.149',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.14',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.183',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.31',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.79',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.138',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.175',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.17',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.158',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.165',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.10',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.48',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.177',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.87',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.152',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.3',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.124',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.45',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.179',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.89',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.83',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.193',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.5',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.36',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.130',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official-1.json.6',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.160',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.188',
    '/mnt/8tb/csenrc/tc3/data/trace/extracted_json/ta1-trace-e3-official.json.99'
]



def task(data_file_list):
    
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

    psql_connection_url = 'postgresql+psycopg2://csephase2:csephase@@localhost/darpa_trace'

    for data_file_name in data_file_list:
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
                        object_holder.append(ps.parse_host(db_object[object_type]))
                        host_count += 1
                    elif object_type[29:] == 'Principal':
                        object_holder.append(ps.parse_principal(db_object[object_type]))
                        principal_count += 1
                    elif object_type[29:] == 'ProvenanceTagNode':
                        object_holder.append(ps.parse_provenance_tag_node(db_object[object_type]))
                        provenance_tag_node_count += 1
                    else:
                        print(object_type[29:])

                    # if len(object_holder)>20:
                    #     break
                except Exception as e:
                    traceback.print_exc()
                    print("\n\n")
                    print(db_object[object_type])
                    #print("Error in parsing line: " + line)
                    break
        
        print("File reading is complete.")

        bulk_dump_in_db(object_holder, psql_connection_url, batch_size=100000)
        object_holder.clear()

        # print("Subject: {}".format(subject_count))
        # print("Event: {}".format(event_count))
        # print("File: {}".format(file_count))
        # print("Packet Socket: {}".format(packet_socket_count))
        # print("Registry: {}".format(registry_object_count))
        # print("Unnamed Pipe: {}".format(unnamed_pipe_object_count))
        # print("Memory: {}".format(memory_object_count))
        # print("Netflow: {}".format(netflow_object_count))
        # print("SrcSink: {}".format(src_sink_object_count))
        # print("Host: {}".format(host_count))
        # print("Principal: {}".format(principal_count))
        # print("Provenance Tag Node: {}".format(provenance_tag_node_count))

start_time = dt.now()
print("Starting time of the program: {}".format(str(start_time)))

total_cpus = psutil.cpu_count()
allocated_cpus = 6
print("Total allocated cpus: {}".format(allocated_cpus))
pool = mp.Pool(allocated_cpus)
for i in pool.imap_unordered(task, [trace_data_files[60:80], trace_data_files[80:100]]):
    print(i)
    continue  










