import boto3
import requests as r
import json
import os, math, sys, random
from flask_restful import Resource, reqparse
from flask import Flask, request, jsonify
from datetime import datetime
import time, redis
from botocore.exceptions import ClientError
from connect import redis_host_dn,  datanode_id, connect_to_host


"""
DataNode is responsible for serving read and write requests from the file 
system’s clients. The DataNodes also perform block creation, deletion, and 
replication upon instruction from the NameNode
"""

bucket_name = 'hdfs-group4'

app = Flask(__name__)

blocks = []

# Env to connect to
default_env = "Dev"


@app.route('/block/<string:file_name>/<string:block_name>', methods =['POST'])
def write_block( file_name ,block_name ):
    """
    Data node is asked to write a part of a file and it gets block data and sends a replica to another data node.
    Used for writing data.
    /block/notes.txt/33

    { "block_body" : "Hello friends", "copy_node": ["dn0","dn1"] }
    
    :param name: file_name 
    :param type: str
        The name of the file

    :param name: block_name 
    :param type: str
        The name of the block


    #Payload
    :param name: block_body 
    :param type: str
        The text of the block

    :param name: copy_node 
    :param type: list
        The name of the node that will copy this block

    :returns:
        Text of Block
        {'hello'}
    """ 
    try:
        response = request.data.decode('utf-8') 
        if response:
            dn = json.loads( response )
            block_text = dn['block_body']
            copy_nodes = dn['copy_node']
            write_file_locally( block_name  , block_text )
            for copy in copy_nodes:
                forward_data( file_name, block_name, copy, block_text)
            blocks = add_to_block_list( block_name )

    except Exception as a:
        return jsonify({"error": "ERROR: {0}".format( str(a) ) })
    return {'message': "Block text written to {0} and block {1}".format( datanode_id, block_name )}


@app.route("/block/<string:block_name>/", methods=["DELETE"])
def delete_block(block_name):
    """ 
        Client asks to delete a block.
        
        :param name: block_name 
        :param type: str
            The name of the block

        :returns: message
            Block 1 deleted from dn0, current blocks [2,3]
            Confirms which block was deleted from which datanode
    """
    file_name = block_name + "_file.txt"
    if delete_file( file_name ):
        blocks_l = remove_from_block_list( block_name )
    else:
        return jsonify({"error": "ERROR: File does not exist." }) 
    return {'message': "Block {0} deleted from {1}, current blocks {2}".format( block_name, datanode_id, blocks_l)}
 

@app.route("/copy_block_data/", methods=["POST"])
def copy_block_data():
    """ 
        Namenode wants to copy underreplicated blocks and it asks this datanode to open and
        send its copy of the block to the other datanode.
        { "block_name": "33",  "datanode_id": "dn1" }
        # Pay load
        :param name: block_name 
        :param type: str
            The name of the block

        :param name: copy_node 
        :param type: int
            The id of the next datanode to be forwarded to
    """
    try:
        response = request.data.decode('utf-8') 
        if response:
            vals = json.loads( response )
            copy_node = vals['copy_node']
            block_id = vals['block_name']
            file_name = block_id + "_file.txt"
            file_content = open( file_name,'rb').read()
            forward_data( file_name, block_id, copy_node, file_content)
        else:
            return jsonify({"error": "ERROR: No response received." })
    except Exception as a:
        return jsonify({"error": str(a) })
    return {'message': "Block data {0} forwarded to {1}".format( file_name, copy_node )}


@app.route('/forward_block/<string:file_name>/<string:block_name>', methods =['POST'])
def get_from_dn( file_name , block_name ):
    """
    Data node is asked to forward a block to another datanode.
    /forward_block/notes.txt/14
    
    :param name: file_name 
    :param type: str
        The name of the file

    :param name: block_name 
    :param type: str
        The name of the block

    # Pay load
    :param name: block_body 
    :param type: str
        The text of the block

    :returns:
        Block text "hello friend" written to blocks [4]
    """ 
    try:
        response = request.data.decode('utf-8') 
        if response:
            dn = json.loads( response )
            block_text = dn['block_body']
            write_file_locally( block_name  , block_text )
            add_to_block_list( block_name )
    except Exception as a:
        return jsonify({"error": str(a) })
    return {'message': "Block text written to dn {0} and block {1}".format( datanode_id, block_name )}


@app.route('/block/<string:block_name>/',methods=['GET'])
def read_block(block_name):
    """
    Data node is asked to read a block
    /block/33/

    :param name: block_name 
    :param type: str
        The name of the block

    :returns: dict
        {Block body: "hello friend" }

    """
    try:
        file_name = block_name + "_file.txt"
        file_content = open(file_name,'rb').read()
    except Exception as a:
        return jsonify({"error":str(a)})
    return {'block_body':file_content}


def delete_file( file_name ):
    """ Delete a file locally, if it exists return True else False """
    if os.path.exists(file_name):
        protected = ["namenode.py", "datanode.py", "heartbeat_report.py"]
        if file_name not in protected:
            os.remove(file_name)
        return True
    else:
        return False

def add_to_block_list(block_name):
    blocks_l = get_from_memory('block_data')
    if not blocks_l:
        blocks_l = []
    if isinstance(block_name, str):
        block_name = block_name.replace('b','').replace('_', '')
    if int(block_name) not in blocks_l:
        blocks_l.append(int(block_name))
    add_to_memory( blocks_l, 'block_data')
    return blocks_l


def remove_from_block_list(block_name):
    blocks_l = get_from_memory('block_data')
    if isinstance(block_name, str):
        block_name = block_name.replace('b','').replace('_', '')
    block_id = int( block_name )
    if block_id in blocks_l:
        blocks_l.remove( block_id )
    add_to_memory( blocks_l, 'block_data')
    return blocks_l


@app.route('/variables/', methods =['GET'])
def endpoint_variables():
    try:
        blocks = get_from_memory('block_data')
    except:
        blocks = []
    return { 'blocks': blocks }


def forward_data( file_name, block_id, copy_node, block_body):
    """ Forward data to the copy node""" 
    route = connect_to_host( copy_node, default_env ) + '/forward_block/'
    print( r.post(route + file_name + "/" + block_id, json={'block_body': block_body}).json() )


@app.route('/', methods =['GET'])
def endpoint():
    return "True"



def write_file_locally( block_name, block_text):
    """
    Used to write a block_text locally on this node
    """
    new_file_name = block_name + "_file.txt"
    text = str(block_text)
    f = open( new_file_name,"w")
    f.write(text)
    f.close()

 
# Redis is an In-Memory but Persistent on Disk DB
r_cache = redis.Redis( host=redis_host_dn, port=6379)


def add_to_memory( obj, var_name ):
    values = json.dumps( obj )
    r_cache.set( var_name, values )


def get_from_memory( var_name ):
    cached_data =  r_cache.get(var_name)
    cached_data_as_str = json.loads( cached_data )
    return cached_data_as_str


if __name__ == '__main__':
    # Save the data node id to share with the heartbeat_datanode.py
    add_to_memory( datanode_id , 'datanode_id')
    add_to_memory( blocks, 'block_data')
    app.run(host='0.0.0.0',port=5000,debug=True)