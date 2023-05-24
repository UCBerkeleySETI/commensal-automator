import redis

from automator import util, redis_util
from automator.logger import log

class Coordinator(object):
    """Coordinator that runs on the headnode and allocates instances to
    recording and processing tasks for each subarray. 
    """

    def __init__(self, config_file):

        config = util.load_config(config_file)
        self.free = config['instances']
        self.streams_per_instance = config['streams_per_instance']
        self.r = redis.StrictRedis(host=config['redis_host'], 
                                   port=config['redis_port'], 
                                   decode_responses=True)

    def start(self):
        """Start the coordinator.
        """
        self.alert('starting up')

    def alert(self, message):
        redis_util.alert(self.red, message, "coordinator")
    
    def annotate(self, tag, text):
        response = util.annotate_grafana(tag, text)
        log.info(f"Annotating Grafana, response: {response}")
