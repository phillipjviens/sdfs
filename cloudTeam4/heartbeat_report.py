import redis, json, time
import requests as r
from connect import connect_to_host, redis_host_dn

"""
Send periodic heartbeats and block reports from the datanode
"""

# Env to connect to
default_env = "Dev"

# Time that Heartbeat is Sent
heartbeat_time = 5

# Time that Block Reports are Sent
block_report_time = 15


def datanode_send_heartbeat(datanode_id):
    """
    Posts a heartbeat to the name node

    :param name: datanode_id 
    :param type: str
        The name of the datanode that is posting the heartbeat
    """
    print("\nStarting a hearbeat")
    route = connect_to_host("name", default_env)   + '/hearbeat/'
    print( r.put( route  , json={'id': datanode_id}).text )


def datanode_send_blockreport(datanode_id, block_data):
    """
    Posts a block report to the name node

    :param name: datanode_id 
    :param type: str
        The name of the datanode that is posting the request

    :param name: block_data 
    :param type: list
        The list of blocks on the datanode
    """
    print("\nStarting a block report")
    route = connect_to_host("name", default_env)   + '/block_report/'
    payload = {'id': datanode_id, 'block_data': block_data}
    print( r.put( route  , json=payload).text )


def periodically_send_data( datanode_id ):
    """
    Periodically sends the heartbeat and block report
    """
    while True:
        # Post the Heartbeat Periodically
        if int(time.time()) % heartbeat_time == 0:
            datanode_send_heartbeat(datanode_id)
        # Post the Block Report Periodically
        if int(time.time()) % block_report_time == 0:
            block_data = get_from_memory('block_data')
            datanode_send_blockreport(datanode_id, block_data)                
        time.sleep(1)



# Redis is an In-Memory but Persistent on Disk DB
r_cache = redis.Redis(host=redis_host_dn,port=6379)


def add_to_memory( obj, var_name ):
    values = json.dumps( obj )
    r_cache.set( var_name, values )


def get_from_memory( var_name ):
    cached_data =  r_cache.get(var_name)
    cached_data_as_str = json.loads( cached_data )
    return cached_data_as_str



if __name__ == '__main__':
    print("Hello " +  redis_host_dn)
    datanode_id = get_from_memory('datanode_id')
    
    # Posts Heart beats and Block reports on a Loop
    periodically_send_data(datanode_id)
    