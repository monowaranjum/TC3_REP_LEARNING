import numpy as np
import os
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json

data_folder = '/mnt/8tb/csenrc/tc3/data/cadets'
schmea_folder = '/mnt/8tb/csenrc/tc3/schema'
exploration_file_name_json = 'ta1-cadets-e3-official.json'
exploration_file_name_bin = 'ta1-cadets-e3-official.bin'
exploration_schema_file = 'TCCDMDatum.avsc'

# schema = avro.schema.parse(open(os.path.join(schmea_folder, exploration_schema_file), "rb").read())

# print(schema)
# print("[")
raw_objects = []
with open(os.path.join(data_folder, exploration_file_name_json), 'r') as f:
    count = 0
    for line in f:
        print(list(json.loads(line)["datum"].keys())[0][29:])
        # print(",")
        #raw_objects.append(json.loads(line))
        count +=1
        if count == 5:
            break 
# print("]")

# print(raw_objects[0]['datum'])


# reader = DataFileReader(open(os.path.join(data_folder, exploration_file_name_bin), "rb"), 
# DatumReader())

# count = 0
# for entry in reader:
#     print(entry)
#     print("\n")
#     count+=1 
#     if count ==3:
#         break

