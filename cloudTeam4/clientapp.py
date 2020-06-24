
import boto3
import logging.handlers
import os, math, sys, random, time
import requests as r
from flask_restful import Resource, reqparse
from flask import Flask,request,jsonify
from datetime import datetime
from botocore.exceptions import ClientError
from connect import connect_to_host
import random

""" 
Client handles the user interactions with the 
system.
"""
# Env to connect to
default_env = "LIVE"

bucket_name = "hdfs-group4"


def start_sufs():
    """ Obtain initial input from client"""
    print("SUFS Initiated... Please indicate whether you'd like to \n")
    
    while True: 
        command = input("\nwrite, read, list, delete, exit, mkdir, rmdir, ls, download, or exit: \n")
        if command == '' or command == 'exit':
            print("Thank You")
            break
        clinetapp_get_user_input(command) 
    return 0


def get_file_size( file_name ):
    cwd = os.getcwd()
    path = cwd + "/" + file_name
    try:
        size = os.path.getsize( path )
        return size
    except OSError as e:
        print( "File not found in the code folder, Please download the file using the download command.")
        return 0


def clientapp_get_block_lists_to_write(directory, file_name, size):
    """ Returns the necessary block information from name_node to write files to SUFS"""
    route = connect_to_host("name", default_env) + '/file/'
    response = r.post(route  , json={'file_name': file_name, 'directory': directory, 'size':size })
    if response:
        response = response.json()
        if "error" in response:
            print("NameNode {0}".format(  response['error'] ))
            return 0
        else: 
            file_size = get_file_size( file_name )
            block_count = int(response['block_count'])
            block_size = int(response['block_size'])
            blocks = response['blocks_and_dns']
            block_data = {}
            block_data['file_name'] = file_name
            block_data['file_size'] = file_size
            block_data['block_count'] = int(block_count)
            block_data['block_size'] = int(block_size)
            block_data['data_blocks'] = blocks
            return block_data
    else:
        print( "ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env ))


def download_file( file_name ):
    """ Downloads a file from s3 returns True if it exist, False otherwise """
    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, file_name, file_name)
        return True
    except:
        return False


def download_is_complete(file_name):
    """Checks if the download is complete before moving on, return True if complete"""
    wait_time = 0
    finished_download = False
    while not finished_download and wait_time < 100:
        finished_download = os.path.exists(file_name)
        time.sleep(1)
    return finished_download


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def client_delete_file_from_namenode(directory, file_name):
    """This function deletes file from nn block list and file dir 
        and returns a list of block_ids where blocks of
        data associated with the given file_name can be passed
        to the delete functions for the datanodes """
    route = connect_to_host("name", default_env) + '/file/'
    response = r.delete(route + directory + "/" + file_name )
    if response:
        return response.json()        
    else:
        print( "ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env ))
        return 0


def clientapp_get_block_names(file_name, directory):
    """This function returns a list of block_ids where blocks of
        data associated with the given file_name can be passed
        to the read functions"""
    route = connect_to_host("name", default_env)  + '/file/'
    response = r.get(route + directory + "/" + file_name)
    if response:
        return response.json()        
    else:
        print( "ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env ))


def remove_directory( directory ):
    """ Removes the specified directory, if it is empty """
    route = connect_to_host( "name", default_env ) + '/directories/'
    response = r.delete( route , json={'directory': directory} )
    if response:
        response = response.json()
        if "error" in response:
            print("Namenode {0}".format(  response['error'] ))
        else:
            print( response['message'] )


def remove_subdir(parent, sub_dir):
    """Removes a child driectory from a parent directory"""
    route = connect_to_host("name", default_env) + '/directories/sub_dir/'
    response = r.delete( route, json={'parent': parent, 'sub_directory': sub_dir} )
    if response:
        response = response.json()
        if "error" in response:
            print("Namenode {0}".format(  response['error'] ))
        else:
            print( response['message']) 
    else:
        print( "ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env ))


def clientapp_write_file(blocks_info):
    """ Write the file to the datanodes """ 
    file_name = blocks_info['file_name']
    block_size = blocks_info['block_size']
    blocks_to_dns = blocks_info['data_blocks']
    # Sort the blocks 
    block_l = []
    for block_id in blocks_to_dns.keys():
        block_l.append( int( block_id ))
    block_l.sort()
    # Read the file in chunks and send to datanodes
    read_blocks_and_send_to_dns( file_name, block_size, block_l , blocks_to_dns )


def write_request_to_dn( cur_data_node, file_name, block_id , copy_node, block_body ):
    route = connect_to_host( cur_data_node, default_env ) + '/block/'
    response = r.post(route + file_name + "/" + str(block_id) , json={'block_body': block_body, 'copy_node': copy_node})
    if response:
        response = response.json()
        if "error" in response:
            print("DataNode {0}".format(  response['error'] ))
        else:
            print( response['message'] )
    else:
        print( "ERROR: DATANODE {0} IS OFFLINE. ENVIRONMENT IS {1}.".format( cur_data_node, default_env ))


def read_blocks_and_send_to_dns( file_name, block_size, sorted_blocks , blocks_to_dns ):
    if download_is_complete( file_name ):
        with open(file_name) as f:
            index = 0
            # For each block chunk write to a datanode
            for piece in read_in_chunks(f, block_size):
                block_id = sorted_blocks[index]
                data_nodes = blocks_to_dns[str(block_id)]
                cur_data_node = data_nodes[0]
                data_nodes.pop(0)
                copy_node = data_nodes
                print("Writing to {0} and copy to {1} ...".format( cur_data_node, copy_node ) )
                write_request_to_dn( cur_data_node, file_name, block_id , copy_node, piece )
                index += 1
    else:
        print("Client ERROR: Download took over 2 minutes.")


def clientapp_delete_file(blocks_dns):
    """
    The delete function reads a list of block_ids from the name node 
    after supplying the file we'd like to delete. the list is copied to a dict
    which is then used to send a delete request to the data_nodes. 
    """ 
    blocks_and_dns = blocks_dns['blocks_and_dns']
    for block_id, data_node_list in blocks_and_dns.items():
        for dn in data_node_list:
            route = connect_to_host( dn, default_env ) + '/block/'
            response = r.delete( route + block_id + "/" )
            if response:
                response = response.json()
                if "error" in response:
                    print( "Datanode {0}: {1}".format( dn, response['error'] ))
                else:
                    print( response['message'] )
            else:
                print( "ERROR: DATANODE {0} IS OFFLINE. ENVIRONMENT IS {1}.".format( dn, default_env ))


def clientapp_read_file(file_name, blocks):
    """
    The read function takes in a file_name and a list of 
    datablocks supplied by the name_node which then sends 
    requests to the corresponding data_nodes to access block
    data.
    """ 
    new_file_name =  "local_" + file_name 
    f = open( new_file_name,"w")
    b = blocks['message']

    for block_id, nodes in b.items():
        block_id = int(block_id)

    for block_id, nodes in sorted(b.items()):
        dn = random.choice(nodes)
        route = connect_to_host(dn, default_env ) + '/block/'
        variables = r.get(route + str(block_id) + '/').json()
        text = str(variables['block_body'])
        f.write(text)
    f.close()


def list_all_in_directory( directory ):
    """ Lists all the files of the specified directory """
    route = connect_to_host( "name", default_env ) + '/directories/'
    response = r.post(route , json={'directory': directory})
    if response:
        response = response.json()
        if "error" in response:
            print("Namenode {0}".format(  response['error'] ))
        else:
            print( response['message'] )
    else:
        print( "ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env ))
 

def clientapp_makedir(directory_name):
    route = connect_to_host("name", default_env) + '/directories/' + directory_name + '/'
    response = r.get( route )
    if response:
        response = response.json()
        if "error" in response:
            print("Namenode {0}".format( response['error'] ))
        else:
            print( response['message'] )
    else:
        print("ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env))


def clientapp_make_sub(parent, directory_name):
    route = connect_to_host("name", default_env) + '/directories/sub/' + parent + '/' + directory_name + '/'
    response = r.get(route)
    if response:
        response = response.json()
        if "error" in response:
            print("Namenode {0}".format( response['error'] ))
        else:
            print( response['message'] )
    else:
        print("ERROR: NAMENODE IS OFFLINE. ENVIRONMENT IS {0}.".format( default_env))


def clientapp_list_blocks(file_name, blocks):
    block_var = blocks['message']
    print("\n\nHere's a list of of the Blocks and Data Nodes associated with")
    print("File: " + file_name + "\n")
    for block_id, nodes in block_var.items():
        print("Block_id: " + block_id)
        for node_id in nodes:
            print("\t\t" + node_id)


def clinetapp_get_user_input(user_input):
    """
    Process user input and decide which funtion user is calling.    
    """
    try:
        if user_input == 'write':
            file_name = input("Please enter the file to write: ")
            directory = input("Enter the path, seperated with \ (ex: test\data): ")
            size = get_file_size( file_name )
            if size > 0:
                block_info = clientapp_get_block_lists_to_write(directory, file_name, size)
                if block_info != 0:
                    clientapp_write_file(block_info)

        elif user_input == "read":
            file_name = input("Please enter the file to read: ")
            directory = input("Enter the path, seperated with \ (ex: test\data): ")
            blocks = clientapp_get_block_names(file_name, directory)
            if "error" in blocks:
                print( blocks["error"] )
            else:
                clientapp_read_file(file_name, blocks)

        elif user_input == "delete":
            file_name = input("Please enter the file to delete: ")
            directory = input("Enter the path, seperated with \ (ex: test\data): ")
            blocks = client_delete_file_from_namenode( directory, file_name )
            if blocks:
                if "error" in blocks:
                    print( blocks["error"] )
                else:
                    print( "Deleting {0} ....".format( blocks['blocks_and_dns'] ))
                    clientapp_delete_file(blocks)
                
        elif user_input == "list":
            file_name = input("Please enter the file to list: ")
            directory = input("Enter the path, seperated with \ (ex: test\data): ")
            blocks = clientapp_get_block_names(file_name, directory)
            if "error" in blocks:
                print(blocks)
            else:
               clientapp_list_blocks(file_name, blocks)

        elif user_input == "mkdir":
            sub = input("is this a sub directory? (y/n) ")
            if sub == 'y' or sub == 'Y':
                parent = input("Please indicate the parent directory: ")
                sub_dir = input("Please indicate the sub directory: ")
                clientapp_make_sub(parent, sub_dir)
            else:
                directory_name = input("Please specify a name for the directory: ")
                clientapp_makedir(directory_name)

        elif user_input == "rmdir":
            sub = input("is this a sub directory? (y/n) ")
            if sub == 'y' or sub == 'Y':
                parent = input("Please specify the parent directory: ")
                sub_dir = input("Please specify the sub_directory: ")
                remove_subdir(parent, sub_dir)
            else:
                directory = input("please specify the directory you'd like to remove: ")
                remove_directory( directory )

        elif user_input == "ls":
            directory = input("Enter the path, seperated with \ (ex: test\data): ")
            list_all_in_directory( directory )
         
        elif user_input == "exit":
            return 0

        elif user_input == 'download':
            file_name = input("Please indicate the file to download from s3: ")
            if download_file( file_name ):
                print( "Downloaded file")
            else:
                print( "Can not download from s3")

        else: 
             print("Please indicate either write, read, list, delete, exit, mkdir, rmdir, ls, download, or exit")
    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == '__main__':
    start_sufs()
