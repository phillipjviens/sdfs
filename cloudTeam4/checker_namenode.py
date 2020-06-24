import redis, json, time
import requests as r
from connect import connect_to_host, redis_host_nn
from namenode import get_next_available_dn, replica_count
from datetime import datetime
from dateutil import parser
from datetime import timedelta

""" 
    Checker for Namenode uses the redis cache to access the block list 
    and file dir then it can run while loops to check for things like 
    under replicated blocks 
    
"""
default_env = "Dev"

# Time that Under Replicated Blocks Should Be Checked 
under_rep_time = 20

# Determines how long before a databode is considered dead
time_past_heartbeat = 20

# Replication Factor
replication_factor = replica_count

# Redis is an In-Memory but Persistent on Disk DB
r_cache = redis.Redis(host=redis_host_nn,port=6379)


def add_to_memory( obj, var_name ):
    values = json.dumps( obj )
    r_cache.set( var_name, values )


def get_from_memory( var_name ):
    cached_data =  r_cache.get(var_name)
    cached_data_as_dict = json.loads( cached_data )
    return cached_data_as_dict


def are_blocks_under_replicated( ):
    """ Check if any blocks are under replicated """
    under_rep_blocks = []
    block_list = get_from_memory('block_list')
    for block, dns in block_list.items(): 
        # the block has less datanodes then the replication num 
        if len(dns) < replication_factor:
            under_rep_blocks.append( block )
    if len(under_rep_blocks) == 0:
        print( "Are they under : No")
    else:
        print( "Are they under : Yes {0}".format( under_rep_blocks ))
    return under_rep_blocks


def any_datanodes_dead():
    """ See if any datanodes are dead"""
    dead = []
    heartbeats = get_from_memory("heartbeats")
    for k, v in heartbeats.items():
        cur_time = datetime.now()
        prev_time = parser.parse(v)
        time_passed =  cur_time - prev_time
        # Have not heard from node for over 30 sec
        if time_passed > timedelta(seconds=time_past_heartbeat):
            print( "Data node {0} is dead.".format( k ))
            # Mark as dead and remove from block list
            dead.append( k )
            remove_dead_dns_from_bl( k )
    return dead
    

def update_block_list_with_reports():
    """ Update block list with block report temp list """
    temp = {}
    block_temp = get_from_memory('block_temp')
    # Use the block reports to update the list
    for dn, blocks in block_temp.items(): 
        for b in blocks:
            # add to the dn list for each block
            temp.setdefault( str(b), [] ).append( dn )
       
    # Check if a block is missing from all block reports
    block_list = get_from_memory('block_list')
    for b, dn in block_list.items():
        if b not in temp:
            temp.setdefault( b, [] )
    try:
        add_to_memory( temp , 'block_list' )
    except redis.exceptions.ReadOnlyError:
        print('\n\n\n\n\n\n\n\n')
        print(block_list)
        raise
    return temp


def check_for_under_replicated_blocks():
    """ 
        Periodically update the block list and check for under replicated blocks 
        new block replicas should be created when the number of block replicas falls
        below N
    """ 
    while True:
        time.sleep( under_rep_time )
        # update your block list with the report info
        block_list_l = update_block_list_with_reports()
        dead_nodes = any_datanodes_dead()

        # check if any blocks are under replicated
        list_under = are_blocks_under_replicated()
        # send message to working datanodes to copy their block to another one
        ask_to_copy_underreplicated_block( list_under , block_list_l )


def remove_dead_dns_from_bl( dead_dn ):
    block_list = get_from_memory('block_list')
    for b, dns in block_list.items():
        for dn in dns:
            if dn == dead_dn:
                dns.remove( dead_dn )
        block_list[b] = dns
    print( "Changed block list {0}".format( block_list ))
    add_to_memory( block_list , 'block_list' )
    

def ask_to_copy_underreplicated_block( list_under , block_list ):
    """ Add calls to the datanodes to copy blocks that are under replicated """
    # check the current dn 
    print( "BLOCKS")
    print( block_list )
    for l in list_under:
        if l in block_list:
            try:
                # Cet the first dn and ask them to forward their copy
                dn_with_block = block_list[l][0]
                print( "Dn with this Block is {0}".format( dn_with_block ))
                copy_node = get_next_available_dn()
                
                # Can't be a node that it is allready on:
                if copy_node == dn_with_block:
                    copy_node = get_next_available_dn()
                print( "Copy block {0} the next available dn {1}".format(l, copy_node ))
                forward_data( dn_with_block, copy_node, str(l))
            except:
                print( "Could not forward the under replicated block.")




def forward_data( dn_with_block, copy_node, block_name ):
    """ Connects to dn and asks it to make a copy to send to the copy_node """
    route = connect_to_host(dn_with_block, default_env)   + '/copy_block_data/'
    response = r.post( route, json={'copy_node': copy_node, 'block_name': block_name})
    if response:
        response = response.json()
        if 'error' in response:
            print( response['error'])
        else:
            print( response['message'])
    else:
        print("ERROR: DATANODE {0} IS OFFLINE. ENVIRONMENT IS {1}".format(copy_node, default_env))


if __name__ == '__main__':
    print("Checking for under replicated blocks...")
    check_for_under_replicated_blocks()
    