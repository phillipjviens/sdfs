import requests as r

""" Connection configuration"""

def connect_to_host( service, environment ):
    if environment == "LIVE":
        return connect_to_host_live( service )
    else:
        return connect_to_host_dev( service )

def connect_to_host_dev( service ):
    if service[0] == "d":
        host = "http://datanodeservice:5000"
    elif service == "client":
        host = "http://clientservice:6000"
    elif service == "name":
        host = "http://namenodeservice:4000"
    try:
        r.get( host ).ok
    except r.exceptions.ConnectionError:
        host = "http://192.168.99.100"
        r.get( host + ":5000" ).ok
    return host

def connect_to_host_live( service ):
    if service == "dn0":
        print( "Live on dn0")
        host = "http://35.165.104.182:5000" 
    elif service == "dn1":
        print( "Live on dn1")
        host = "http://52.12.167.133:5000"
    elif service == "dn2":
        print( "Live on dn2")
        host = "http://54.213.245.226:5000" 
    elif service == "client":
        host = "http://52.38.61.85:4000"
    elif service == "dn3":
        print("Live on dn3")
        host = "http://52.26.135.106:4000"
    elif service == "name":
        host = "http://54.188.135.209:4000"
    return host

# Current DN Configs
redis_host_dn = 'redisservice' #'34.215.63.33'
redis_host_nn = 'redisservice' #'54.188.135.209'
datanode_id = "dn0"