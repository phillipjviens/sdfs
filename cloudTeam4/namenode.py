import boto3
import os, math, sys, random
from flask import Flask, request, jsonify
from datetime import datetime
from dateutil import parser
import requests as r
from datetime import timedelta
import time, redis, json
from botocore.exceptions import ClientError
from connect import redis_host_nn

""" 
Namenode is the center piece. It keeps the directory tree 
of all files in the file system, and tracks where across the 
cluster the file data is kept.
"""

# Trash Directory < key: file_name, val: dict of < block numbers, list of data nodes> >
trash_dir = dict()

# Block size
block_size = int(1.024e7 * 1.28) 
#block_size = int(1.024e8 * 1.28) 

# Time created
time_created = datetime.now()

# S3 Bucket name
bucket_name = 'hdfs-group4'

# Number of Replicas 
replica_count = 3

# App 
app = Flask("namenode")

# Redis is an In-Memory but Persistent on Disk DB
r_cache = redis.Redis(host=redis_host_nn,port=6379)

# Listen for Heart Beats 
@app.route("/hearbeat/", methods=["PUT"])
def receive_hearbeat():
    """
    Listen for heart beats from the datanodes and save the most recent heartbeat for each node
    """
    response = request.data.decode('utf-8') 
    print( response )
    if response:
        datanode_id = json.loads( response )
        dn = datanode_id['id']
        dns = NodeAvailability
        dns.update_heartbeat( dn )
        heartbeats = dns.get_heartbeat()
        add_to_memory( heartbeats, 'heartbeats')
        return 'Received heartbeat from {0} on {1}'.format( dn , heartbeats[dn] )
    else:
        return jsonify({"error": "ERROR: No heartbeat received" })


# Listen for Block Reports
@app.route('/block_report/', methods=['PUT'])
def listen_for_block_report():
    """ 
    Listen to the datanode for the block report and receives the id and blocks 
    """
    response = request.data.decode('utf-8') 
    if response:
        datanode = json.loads( response )
        dn = datanode['id']
        blocks_on_dn = datanode['block_data']
        block_temp = get_from_memory('block_temp')
        block_temp[dn] = blocks_on_dn
        add_to_memory( block_temp, 'block_temp')
        return "Received block report for {0} on datanode {1} ".format( blocks_on_dn, dn )
    else:
        return jsonify({"error": "ERROR: No block report received" })


@app.route('/file/', methods =['POST'])
def identify_blocks():
    """
    Name node is asked by Client to write file so it returns the block numbers
    /file/

    # Payload
    :param name: file_name 
    :param type: str
        The name of the file that the client wants to write that is in s3

    :param name: directory 
    :param type: str
        The name of the directory

    :param name: size 
    :param type: int
        The size of file

    :returns: dict
        The block count, block size, and identifying the number of blocks and associted datanodes
        {
            "block_count": "1",
            "block_size": "134217728",
            "blocks_and_dns": {
                "0": [
                "dn1",
                "dn4"
                ]
            }
        }
    """
    try:
        response = request.data.decode('utf-8') 
        if response:
            val = json.loads( response )
            file_name = val['file_name']
            directory = val['directory']
            size =  val['size']

            if directory not in directories:
                return jsonify({"error": "ERROR: {0} not in directory. Use command mkdir.".format(directory)})

            # Can only write a file that is not allready in the file system
            file_dir = get_from_memory('file_dir')
            if file_name not in file_dir: 
                block_count = decide_number_of_blocks(size)
                blocks_and_dns = decide_which_nodes( file_name, block_count, directory )
            else:
                return jsonify({"error": "ERROR: That file already exists in the system please delete it first: {0}".format( file_name ) })
    except Exception as a:
            return jsonify({"error": "ERROR: {0}".format( str(a) ) })
    return {'block_count': "{0}".format(block_count), 'block_size': "{0}".format(block_size), 'blocks_and_dns': blocks_and_dns }



@app.route('/file/<string:directory>/<string:file_name>', methods =['GET'])
def send_block_names(directory, file_name):
    """
    Name node is asked by Client to read file so it returns a list of associated blocks and datanodes.  
    /file/values_dir/notes.txt

    :param name: directory 
    :param type: str
        The name of the directory

    :param name: file_name 
    :param type: str
        The name of the file that the client wants to read from datanodes

    :returns: dict
        The blocks and the associated data nodes for the file
        { "message": {"3": ["dn0", "dn2"]} }    
    """
    try:
        # lookup file directory with map between file and blocks
        file_dir = get_from_memory('file_dir')
        if file_name in file_dir:
            blocks_and_dir =  file_dir[file_name]
            if directory in blocks_and_dir:
                blocks = blocks_and_dir[directory]
                dn_for_file = dict()
                block_list = get_from_memory('block_list')

                # for each block add the list of data nodes it resides on to the dict
                for b in blocks:
                    b = str(b)
                    if b in block_list:
                        dn_list = block_list[b]
                        dn_for_file.setdefault( b, [] ).extend( dn_list )
                    else:
                        # blocks associated w/ file not found in block list
                        error = "block list: {0}  missing: {1}".format( block_list , b )
                        return jsonify({"error": "ERROR: Blocks are missing - {0}".format( error )})
                return { 'message': dn_for_file }
            else:
                return jsonify({"error": "ERROR: The file is not in that directory."})
        else:
            return jsonify({"error": "ERROR: That file could not be found - file: {0}".format( file_name ) })
    except Exception as a:
        return jsonify({"error": str(a) })


@app.route('/file/<string:directory>/<string:file_name>', methods =['DELETE'])
def delete_file(directory, file_name):
    """
    Name node is asked by client to delete file so it sends the associated blocks
    and datanodes.  
    /file/directory/check.txt

    :param name: file_name 
    :param type: str
        The name of the file that the client wants to delete from datanodes

    :param name: directory 
    :param type: str
        The name of the directory

    :returns: dict
        The blocks and data nodes related to the file to be deleted.
        { "blocks_and_dns": "{'4': ['dn0', 'dn1'], '5': ['dn1', 'dn2']}"}
    """
    try:
        file_dir = get_from_memory('file_dir')
        if file_name in file_dir:
            blocks_and_dir =  file_dir[file_name]
            if directory in blocks_and_dir:
                blocks = blocks_and_dir[directory]
                blocks_and_dns = delete_from_block_list( blocks )
                delete_from_file_dir( file_name )
                return {'blocks_and_dns': blocks_and_dns }
            else:
                return jsonify({"error": "ERROR: That file is not in that directory."})

        else:
            return jsonify({"error": "ERROR: That file could not be found - file: {0}  current files: {1}".format( file_name, file_dir ) })
    except Exception as a:
        return jsonify({"error": "Unkown"})


@app.route('/directories/sub_dir/', methods =['DELETE'])
def delete_sub():
    try: 
        response = request.data.decode('utf-8') 
        val = json.loads( response )
        parent = val['parent']
        sub_directory = val['sub_directory']

        dirs = get_from_memory('file_dir')
        key = "dir " + sub_directory
        if key in dirs:
            subs_and_dir = dirs[key]
            if parent in subs_and_dir:
                delete_from_file_dir( key )
                path = parent + "\\" + sub_directory
                directories.remove( path )
                return {'message': "NameNode Sub_directory {0} deleted".format(sub_directory)}
            else:
                return jsonify({"error": "ERROR: That sub_directory is not in that directory."})
        else:
            return jsonify({"error": "ERROR: That file could not be found - file: {0}  current files: {1}".format( sub_directory, dirs ) })
    except Exception as a:
        return jsonify({"error": "ERROR: {0}".format( a )})

        

@app.route('/directories/', methods =['DELETE'])
def delete_directories():
    """
    Name node deletes the directory if it is empty 

    :param name: directory 
    :param type: str
        The name of the directory

    :returns:  dict
        {'dns_with_dir' : ['dn0', 'dn1'] }
    """
    response = request.data.decode('utf-8') 
    if response:
        val = json.loads( response )
        directory = val['directory']

        # Delete directory only if it exits
        if directory in directories:
            # Check if the directory is empty
            file_dir = get_from_memory('file_dir')
            current_dir = []
            for file_name, dir_and_blocks in file_dir.items():
                dirs = dir_and_blocks.keys()
                for d in dirs:
                    if d == directory:
                        current_dir.append( file_name )
            if len( current_dir ) > 0 :
                return { 'error': "ERROR: The {0} directory is Not Empty. It currently contains {1}".format( directory, current_dir ) }
            else:
                directories.remove(directory)
                path = directory.split("\\")
                last_dir = path.pop()
                dirs = get_from_memory('file_dir')
                key = "dir " + last_dir
                if key in dirs:
                    delete_from_file_dir( key )
                return { 'message': "Directory {0} removed successfully, with path .".format( key, directory ) }
        else:
            return jsonify({"error": "ERROR: The directory {0} does not exist.  Current directories are {1}".format( directory, directories ) })
    else:
        return { 'error': "ERROR: No response." }


def get_data_nodes_for_blocks( blocks ):
    block_list = get_from_memory('block_list')
    dns_with_blocks = set()
    for b in blocks:
        b = str(b)
        if b in block_list:
            dns = block_list[b]
            dns_with_blocks |= set(dns)
    return list(dns_with_blocks)


@app.route('/directories/<string:directory>/', methods =['GET'])
def make_directories(directory):
    """
    Name node makes the directory
    
    :param name: directory 
    :param type: str
        The name of the directory

    :returns:  dict
        {'message' : 'Made directory'}
    """
    if directory not in directories:
        path = directory.split("\\")

        # Add to directories
        sub = ""
        for d in range( len(path) ):
            if len(sub) > 0:
                sub += "\\" +  path[d]
            else:
                sub = path[d]
            directories.append(sub)

        # Add to file dir
        file_dir = get_from_memory('file_dir')
        parent = path.pop(0)
        subs = "dir " + '\\'.join(path)
        if subs not in file_dir:
            add_sub_to_file_dir(subs, parent)

        return { 'message': "Made directory {0}".format( directory ) }
    else:
        return { 'error': "ERROR: The {0} directory allready exists.".format( directory ) }


@app.route('/directories/sub/<string:parent>/<string:directory>/', methods =['GET'])
def make_sub_directory(parent, directory):
    """
    Name node makes the directory
    
    
    :param name: parent 
    :param type: str
        The name of the parent directory

    :param name: directory 
    :param type: str
        The name of the sub directory

    :returns:  dict
        {'message' : 'Made directory'}
    """
    if parent not in directories:
        directories.append(parent)
    path = parent + '\\' + directory 
    if path not in directories:
        directories.append(path)
    file_dir = get_from_memory('file_dir')
    if path not in file_dir:
        directory = "dir " + directory
        add_sub_to_file_dir(directory, parent)
        return { 'message': "Made sub directory {0}".format( path ) }
    else:
        return { 'error': "ERROR: The {0} directory allready exists.".format( path ) }

    

@app.route('/directories/', methods =['POST'])
def get_directories():
    """
    Name node lists all the files of the specified directory
    
    # Payload
    :param name: directory 
    :param type: str
        The name of the directory

    :returns:  dict
        {'message' : 'DIR: name  -- files'}
    """
    pretty_print = ""
    response = request.data.decode('utf-8') 
    if response:
        val = json.loads( response )
        directory = val['directory']

        file_dir = get_from_memory('file_dir')
        pretty_dir = {} 
        for file_name, dir_and_blocks in file_dir.items():
            dirs = dir_and_blocks.keys()
            for d in dirs:
                if d == directory:
                    pretty_dir.setdefault( d, [] ).append( file_name )
        
        for d , files in pretty_dir.items():
            pretty_print += "DIR: {0}\n".format( d) 
            for f in files:
                pretty_print += "  -- {0}\n".format( f )
    return { "message" : pretty_print }


def delete_from_block_list(blocks_to_delete):
    """ Deletes from block list and returns list of blocks and datanodes for client"""
    block_list = get_from_memory( 'block_list')
    send_to_client = {}
    for b in blocks_to_delete:
        key = str(b)
        if key in block_list:
            send_to_client[key] = block_list[key]
            del block_list[key]
    add_to_memory( block_list, 'block_list' )
    return send_to_client


def delete_from_file_dir( file_name ):
    file_dir_l = get_from_memory( 'file_dir')
    del file_dir_l[file_name]
    add_to_memory( file_dir_l, 'file_dir' )


@app.route('/', methods =['GET'])
def endpoint_test():
    return "True"


@app.route('/variables/', methods =['GET'])
def endpoint_variables():
    block_list_l = get_from_memory('block_list')
    file_dir_l = get_from_memory('file_dir')
    return {'file_dir': file_dir_l, 'block_list': block_list_l , 'directories': directories }


def decide_number_of_blocks( size ):
    """
    Calculate the number of blocks needed based on the file size
    """
    block_num = math.ceil( size / block_size )
    return block_num
    
    
def add_to_block_list( blocks_and_dns ):
    block_list = get_from_memory('block_list')
    for b, dns in blocks_and_dns.items():
        block =  int(b)
        if b in block_list:
            for dn in dns:
                if dn not in block_list[b]:
                    block_list[b].append( dn )
        else:
            block_list.setdefault( block, dns )
    add_to_memory( block_list, 'block_list')


def add_to_file_dir( file_name, blocks, directory ):
    """ Adds a given file and list of blocks to file_dir """
    # Does not add if the file is allready in the file system
    file_dir_l = get_from_memory('file_dir')
    if directory not in directories:
        directories.append( directory )

    if file_name not in file_dir_l:
        file_dir_l[file_name] = { directory: blocks }
        add_to_memory( file_dir_l, 'file_dir' )

def add_sub_to_file_dir(sub_directory, directory):
    """ Adds a given sub_directory and list of blocks to file_dir """
    file_dir_l = get_from_memory('file_dir')
    if sub_directory not in file_dir_l:
        file_dir_l[sub_directory] = {directory: sub_directory}
        add_to_memory(file_dir_l, 'file_dir')

def decide_which_nodes( file_name, block_count, directory ):
    """
    Decide which nodes get the file based on number of blocks
    , the available nodes, and the next block id available
    """    
    blocks_and_dns = {}
    all_blocks_for_file = []
    for b in range(block_count):
        replicas = []
        # Get an available datanode replica for each block
        for i in range(replica_count):
            next_dn = get_next_available_dn()
            replicas.append(next_dn)
        # Get next available block id that has not been used
        next_block_id = b  + get_next_block_id() 
        all_blocks_for_file.append( next_block_id )

        blocks_and_dns.update( { next_block_id : replicas })
        print( blocks_and_dns )
    # update local block list and file dir
    add_to_block_list( blocks_and_dns )
    add_to_file_dir( file_name, all_blocks_for_file, directory )
    return blocks_and_dns


def get_next_available_dn():
    """ Get the next available node based on heartbeats """
    next_dn = NodeAvailability.get_next_node()
    return next_dn


def get_next_block_id():
    """ Return next block id not used """
    block_list = get_from_memory( 'block_list' )
    if len(block_list) > 0:
        max_block_id = int(max(block_list, key=int))
        max_block_id += 1
        return max_block_id
    else:
        return 1


def add_to_memory( obj, var_name ):
    values = json.dumps( obj )
    r_cache.set( var_name, values )


def get_from_memory( var_name ):
    cached_data =  r_cache.get(var_name)
    try:
        cached_data_as_dict = json.loads( cached_data )
    except TypeError:
        print('\n\n\n\n\n\n\n\n')
        print(cached_data)
        raise
    return cached_data_as_dict


class NodeAvailability:
    """ Knows the next node and available nodes """
    # Available data nodes < key: datanode name, val: available T or F >
    data_nodes = {'dn0':True, 'dn1':True, 'dn2':True }
    next_node = 0
    heartbeats = {}
    time_past_heartbeat = 30
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            print('Creating singleton object')
            cls._instance = super(NodeAvailability, cls).__new__(cls)
        return cls._instance

    def get_next_node():
        # Check availability based on heartbeats
        NodeAvailability.check_node_avail()
        dn = NodeAvailability.next_node_itr()
        avail = NodeAvailability.data_nodes[dn]
        while not avail:
            dn = NodeAvailability.next_node_itr()
            avail = NodeAvailability.data_nodes[dn]
        return dn

    def next_node_itr():
        if NodeAvailability.next_node == len( NodeAvailability.data_nodes ) - 1:
            NodeAvailability.next_node = 0
        else:
            NodeAvailability.next_node += 1
        return 'dn'+ str(NodeAvailability.next_node)

    def update_heartbeat( datanode_id ):
        NodeAvailability.heartbeats.update( {datanode_id: str(datetime.now())} )
    
    def check_node_avail():
        for k, v in NodeAvailability.heartbeats.items():
            cur_time = datetime.now()
            prev_time = parser.parse(v)
            time_passed =  cur_time - prev_time
            # Have not heard from node for over 30 sec
            if time_passed > timedelta(seconds=NodeAvailability.time_past_heartbeat):
                # Mark as Unavailable
                NodeAvailability.data_nodes.update( {k: False } )
            else:
                NodeAvailability.data_nodes.update( {k: True } )

    def get_heartbeat():
        NodeAvailability.check_node_avail()
        return NodeAvailability.heartbeats


# Block List < key: block number, val: list of data node numbers >
block_list = dict()

# Temp data from Block Reports, this updates the block list periodically
block_temp = dict()

# File Directory  < key: file_name, val: list of block numbers >
file_dir = dict()

# Directories 
directories = []

# Initial heartbeats
heartbeats = {}

add_to_memory( block_list, 'block_list')
add_to_memory( block_temp, 'block_temp')
add_to_memory( file_dir, 'file_dir' )
add_to_memory( heartbeats, 'heartbeats')


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=4000,debug=True)
