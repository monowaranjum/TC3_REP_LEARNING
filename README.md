# TC3_REP_LEARNING
NRC_WINTER_2022

Usage:

Please run the files in the following order to construct the graph from scratch. 
The following steps assume that you have postgreSQL installation in your system with python 3.8+. 

- run data_dumping.py first. This will use parser.py to parse the json objects into orm classes and then trannsfer them in db.
- run index_creation.py. This will create an uuid to type index and store in a json file for future use.
- run graph_construction.py. This will create the graph in adjacency list format and store in a file. (Took 26 hours and 32GB memory)

Please use the db_classes.py file to query the database in ORM-based fashion. 

Please use the annotated comments in the files to understand the cleaning,pruning and feature selection.

Contact: monowar.a.1205022@gmail.com
