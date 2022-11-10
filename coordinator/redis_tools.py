from automator.logger import log

class REDIS_CHANNELS:
    """Redis channels for communication between processes.
    
    alerts: General alerts channel - observation stage signals from
            CAM are published here (such as "configure:array_1").

    sensor_alerts: Channel for asynchronously updated sensor values.

    triggermode: Channel for controlling the coordinator's trigger 
                 mode (idle, armed or auto - see coordinator for a 
                 more detailed explanation).
    """
    alerts = "alerts"
    sensor_alerts = "sensor_alerts" 
    trigger_mode = "coordinator:trigger_mode"

def write_pair_redis(server, key, value, expiration=None):
    """Writes a key-value pair to Redis.

    Args:
        server (redis.StrictRedis): A redis-py Redis server object.
        key (str): The key of the key-value pair.
        value (str): The value of the key-value pair.
        expiration (int): Number of seconds before key expiration.

    Returns:
        True if success, False otherwise, and logs either a 'debug' 
        or an 'error' message.

    Examples:
        >>> server = BLBackendInterface('localhost', 5000)
        >>> server._write_to_redis("aliens:found", "yes")
    """
    try:
        server.set(key, value, ex=expiration)
        log.debug("Created redis key/value: {} --> {}".format(key, value))
        return True
    except:
        log.error("Failed to create redis key/value pair")
        return False

def write_list_redis(server, key, values):
    """Creates a new Redis list and rpushes values to it. If a list 
       already exists at the given key, then delete it and rpush values 
       to a new empty list.

       Args:
           server (redis.StrictRedis): A redis-py redis server object.
           key (str): Key identifying the list.
           values (list): List of values to rpush to redis list.

       Returns:
           True if success, False otherwise, and logs either an 'debug' 
           or 'error' message.
    """
    try:
        if server.exists(key):
            server.delete(key)
        server.rpush(key, *values)
        log.debug("Pushed to list: {} --> {}".format(key, values))
        return True
    except:
        log.error("Failed to rpush to {}".format(key))
        return False

def publish_to_redis(server, channel, message):
    """Publishes a message to a redis pub/sub channel associated with 
       a specified Redis server.

    Args:
        server (redis.StrictRedis): A redis-py redis server object.
        channel (str): The name of the channel to publish to.
        message (str): The message to be published.

    Returns:
        True if success, False otherwise, and logs either an 'debug' 
        or 'error' message.

    Examples:
        >>> server = BLBackendInterface('localhost', 5000)
        >>> server._publish_to_redis("alerts", "Found aliens!!!")
    """
    try:
        server.publish(channel, message)
        log.debug("Published to {} --> {}".format(channel, message))
        return True
    except:
        log.error("Failed to publish to {} --> {}".format(channel, message))
        return False
