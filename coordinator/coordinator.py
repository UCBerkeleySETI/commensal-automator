import time
from datetime import datetime, timedelta
import threading
import yaml
import json
import logging
import sys
import redis
import numpy as np
import string
import ast

from .redis_tools import REDIS_CHANNELS, write_list_redis
from .telstate_interface import TelstateInterface
from automator.logger import log, set_logger
from automator import redis_util, util

# Redis channels
ALERTS_CHANNEL = REDIS_CHANNELS.alerts
SENSOR_CHANNEL = REDIS_CHANNELS.sensor_alerts
TRIGGER_CHANNEL = REDIS_CHANNELS.trigger_mode
TARGETS_CHANNEL = 'target-selector:new-pointing'

# Standard DWELL time to fill the buffers:
DEFAULT_DWELL = 290
# Primary time dwell for use with nshot:
PRIMARY_TIME_DWELL = 30

# Type of stream
STREAM_TYPE = 'cbf.antenna_channelised_voltage'
# F-engine mode (so far 'wide' and 'narrow' are known to be available)
FENG_TYPE = 'wide.antenna-channelised-voltage'
# Hashpipe-Redis gateway domain
HPGDOMAIN   = 'bluse'
# Safety margin for setting index of first packet to record.
PKTIDX_MARGIN = 2048
# Location to save calibration files for diagnostic purposes
DIAGNOSTIC_LOC = '/home/obs/calibration_data' 
# Unique telescope ID:
TELESCOPE_NAME = 'MeerKAT'

class Coordinator(object):
    """This class is used to coordinate receiving and recording F-engine data
       during commensal observations with MeerKAT. It communicates with the 
       processing nodes via the Hashpipe-Redis gateway [1]. 

       The coordinator automatically assigns computing resources to each 
       subarray, and responds to observation stages for each. As the required 
       metadata becomes available, it is published to the appropriate processing
       nodes. 
   
       [1] For further information on the Hashpipe-Redis gateway messages, please 
       see appendix B in https://arxiv.org/pdf/1906.07391.pdf
    """

    def __init__(self, redis_endpoint, cfg_file, trigger_mode):
        """Initialise the coordinator.

           Args:
               redis_endpoint (str): of the format <host>:<port> 
               cfg_file (str): path to the .yml configuration file which
               (among other things) provides a list of available hosts. 
               trigger_mode (str): the desired default trigger mode on startup.  
               This is of the form:

                   \'nshot:<n>\': PKTSTART will be sent for <n> tracked targets. 
                   Thereafter, the trigger mode state will transition to nshot:0
                   and no further recording will take place. 

               Note that this default trigger mode is applied to all future subarrays 
               (unless changed). The trigger mode for specific subarrays can be changed
               as follows by publishing the new trigger mode to the trigger channel, 
               for example:
                      
                   PUBLISH coordinator:trigger_mode coordinator:trigger_mode:<array_name>:nshot:<n>
        """
        self.redis_endpoint = redis_endpoint
        redis_host = redis_endpoint.split(':')[0]
        redis_port = redis_endpoint.split(':')[1]
        self.red = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.cfg_file = cfg_file
        # Read global trigger mode:
        self.trigger_mode = trigger_mode # This is the default trigger_mode (for all subarrays)
        log = set_logger(level=logging.DEBUG)

    def alert(self, message):
        redis_util.alert(self.red, message, "coordinator")
    
    def annotate(self, tag, text):
        response = util.annotate_grafana(tag, text)
        log.info('Annotating Grafana, response: {}'.format(response))
        
    def start(self):
        """Start the coordinator as follows:

           - The list of available Hashpipe instances and the number of streams per 
             instance are retrieved from the main configuration .yml file. 

           - The number of available instances/hosts is read from the appropriate Redis key. 
           
           - Subscriptions are made to the three Redis channels.  
           
           - Incoming messages trigger the appropriate function for the stage of the 
             observation. The contents of the messages (if any) are sent to the 
             appropriate function. 
        """        
        # Configure coordinator
        self.alert('starting up')
        try:
            self.hashpipe_instances, self.streams_per_instance = self.config(self.cfg_file)
            log.info('Configured from {}'.format(self.cfg_file))
        except:
            log.warning('Configuration not updated; old configuration might be present.')
        # Attempt to read list of available hosts. If key does not exist, recreate from 
        # config file
        free_hosts = self.red.lrange('coordinator:free_hosts', 0, 
                self.red.llen('coordinator:free_hosts'))
        if len(free_hosts) == 0:
            write_list_redis(self.red, 'coordinator:free_hosts', self.hashpipe_instances)
            log.info('First configuration - no list of available hosts. Retrieving from config file.')
        # Subscribe to the required Redis channels.
        ps = self.red.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(ALERTS_CHANNEL)
        ps.subscribe(SENSOR_CHANNEL)
        ps.subscribe(TRIGGER_CHANNEL)
        # Process incoming Redis messages:
        try:
            for msg in ps.listen():
                msg_type, description, value = self.parse_redis_msg(msg)
                # If trigger mode is changed on the fly:
                # (Note this overwrites the default trigger_mode)
                if (msg_type == 'coordinator') and (description == 'trigger_mode'):
                    trigger_key = value.split(':', 1)[0] 
                    trigger_value = value.split(':', 1)[1]
                    # Update the trigger mode for the specific array in question:
                    # (this is useful during an observation)
                    self.red.set('coordinator:trigger_mode:{}'.format(trigger_key), trigger_value)
                    log.info('Trigger mode for {}  set to \'{}\''.format(trigger_key, trigger_value))
                # If all the sensor values required on configure have been
                # successfully fetched by the katportalserver
                elif msg_type == 'conf_complete':
                    self.conf_complete(description)
                # If the current subarray is deconfigured, instruct processing nodes
                # to unsubscribe from their respective streams.
                # Only instruct processing nodes in the current subarray to unsubscribe.
                # Likewise, release hosts only for the current subarray. 
                elif msg_type == 'deconfigure':
                    self.deconfigure(description) 
                # Handle the full data-suspect bitmask, one bit per polarisation
                # per F-engine.
                elif msg_type == 'data-suspect':
                    self.data_suspect(description, value)
                # If the current subarray has transitioned to 'track' - that is, 
                # the antennas are on source and trackign successfully. 
                elif msg_type == 'tracking':
                    # Note that the description field is equivalent to product_id 
                    # here:
                    self.tracking_start(description)
                # If the current subarray transitions out of the tracking state:
                elif msg_type == 'not-tracking':
                    self.tracking_stop(description) 
                # If pointing updates are received during tracking
                elif 'pos_request_base' in description:
                    self.pointing_update(msg_type, description, value)
                # Updates to DUT1 (ie UT1-UTC)
                elif 'offset_ut1' in description:
                    self.offset_ut(msg_type, value)
        except KeyboardInterrupt:
            log.info("Stopping coordinator")
            sys.exit(0)
        except Exception as e:
            log.error(e)
            sys.exit(1)
    
    def conf_complete(self, description):
        """This function is run when a new subarray is configured and the 
           katportal_server has retrieved all the associated metadata required 
           for the processing nodes to ingest and record data from the F-engines. 

           The required metadata is published to the Hashpipe-Redis gateway in 
           the key-value pair format described in Appendix B of: 
           https://arxiv.org/pdf/1906.07391.pdf

           Notably, the DESTIP value is set for each processing node - the IP 
           address of the multicast group it is to join. 

           Args:
               
               description (str): the second field of the Redis message, which 
               in this case is the name of the current subarray. 
        """
        # This is the identifier for the subarray that has completed configuration.
        product_id = description
        # Configuration process started:
        self.annotate(
            'CONFIGURE', 
            '{}: Coordinator configuring DAQs.'.format(product_id)
            )
        log.info('New subarray built: {}'.format(product_id))
        tracking = 0 # Initialise tracking state to 0
        # Initialise cal_solutions timestamp to 0 to ensure the most recent
        # cal solutions are recorded. Note using Redis here so that the value persists
        # even if the coordinator is restarted in the middle of an observation. 
        self.red.set('coordinator:cal_ts:{}'.format(product_id), 0)
        # Get IP address offset (if there is one) for ingesting only a specific
        # portion of the full band.
        offset = self.ip_offset(product_id)
        # Initialise trigger mode
        if self.red.get('coordinator:trigger_mode:{}'.format(description)) is None:
            log.info('No trigger mode found on configuration, defaulting to {}'.format(self.trigger_mode))
            self.red.set('coordinator:trigger_mode:{}'.format(description), self.trigger_mode)
        # Generate list of stream IP addresses and publish appropriate messages to 
        # processing nodes:
        addr_list, port, n_addrs, n_red_chans = self.ip_addresses(product_id, offset)
        # Allocate hosts for the current subarray:
        if self.red.exists('coordinator:free_hosts'): # If key exists, there are free hosts
            self.alert('Allocating hosts to {}'.format(product_id))
            # Host allocation:
            free_hosts = self.red.lrange('coordinator:free_hosts', 0, 
                self.red.llen('coordinator:free_hosts'))
            allocated_hosts = free_hosts[0:n_red_chans]
            write_list_redis(self.red, 
                    'coordinator:allocated_hosts:{}'.format(product_id), allocated_hosts)
            # Remove allocated hosts from list of available hosts
            # NOTE: in future, append/pop with Redis commands instead of write_list_redis
            if len(free_hosts) < n_red_chans:
                log.warning("Insufficient resources to process full band for {}".format(product_id))
                # Delete the key (no empty lists in Redis)
                self.red.delete('coordinator:free_hosts')
            elif len(free_hosts) == n_red_chans:
                # Delete the key (no free hosts)
                self.red.delete('coordinator:free_hosts')
            elif len(free_hosts) > n_red_chans:
                free_hosts = free_hosts[n_red_chans:]
                write_list_redis(self.red, 'coordinator:free_hosts', free_hosts)
            log.info('Allocated {} hosts to {}'.format(n_red_chans, product_id))
            # Create Hashpipe-Redis Gateway group for the current subarray:
            # Using groups feature (please see rb-hashpipe documentation).
            # These groups will be given the name of the current subarray. 
            # The groups can be addressed as follows: <HPGDOMAIN>:<group>///set
            for i in range(len(allocated_hosts)):
                hpg_gateway = '{}://{}/gateway'.format(HPGDOMAIN, allocated_hosts[i])
                self.pub_gateway_msg(self.red, hpg_gateway, 'join', product_id, log, True)
            # Apply to processing nodes
            subarray_group = '{}:{}///set'.format(HPGDOMAIN, product_id)

            # Name of current subarray (SUBARRAY)
            self.pub_gateway_msg(self.red, subarray_group, 'SUBARRAY', product_id, log, True)
            # Port (BINDPORT)
            self.pub_gateway_msg(self.red, subarray_group, 'BINDPORT', port, log, True)        
            # Total number of streams (FENSTRM)
            self.pub_gateway_msg(self.red, subarray_group, 'FENSTRM', n_addrs, log, True)
            # Sync time (UNIX, seconds)
            t_sync = self.sync_time(product_id)
            self.pub_gateway_msg(self.red, subarray_group, 'SYNCTIME', t_sync, log, True)
            self.red.set('{}:synctime'.format(product_id), t_sync)
            # Centre frequency (FECENTER)
            fecenter = self.centre_freq(product_id) 
            self.pub_gateway_msg(self.red, subarray_group, 'FECENTER', fecenter, log, True)
            # Total number of frequency channels (FENCHAN)    
            n_freq_chans = self.red.get('{}:n_channels'.format(product_id))
            self.red.set('{}:fenchan'.format(product_id), n_freq_chans)
            self.pub_gateway_msg(self.red, subarray_group, 'FENCHAN', n_freq_chans, log, True)
            # Coarse channel bandwidth (from F engines)
            # Note: no sign information! 
            # (CHAN_BW)
            chan_bw = self.coarse_chan_bw(product_id, n_freq_chans)
            self.red.set('{}:chan_bw'.format(product_id), chan_bw)
            self.pub_gateway_msg(self.red, subarray_group, 'CHAN_BW', chan_bw, log, True) 
            # Number of channels per substream (HNCHAN)
            hnchan = self.chan_per_substream(product_id)
            self.pub_gateway_msg(self.red, subarray_group, 'HNCHAN', hnchan, log, True)
            # Number of spectra per heap (HNTIME)
            hntime = self.spectra_per_heap(product_id)
            self.pub_gateway_msg(self.red, subarray_group, 'HNTIME', hntime, log, True)
            # Number of ADC samples per heap (HCLOCKS)
            adc_per_heap = self.samples_per_heap(product_id, hntime)
            self.red.set('{}:hclocks'.format(product_id), adc_per_heap)
            self.pub_gateway_msg(self.red, subarray_group, 'HCLOCKS', adc_per_heap, log, True)
            # Number of antennas (NANTS)
            n_ants = self.antennas(product_id)
            self.pub_gateway_msg(self.red, subarray_group, 'NANTS', n_ants, log, True)
            # Set PKTSTART to 0 on configure
            self.pub_gateway_msg(self.red, subarray_group, 'PKTSTART', 0, log, True)

            # Individually address processing nodes in subarray group where necessary:
            # Build list of Hashpipe-Redis Gateway channels to publish to:
            chan_list = redis_util.channel_list(HPGDOMAIN, allocated_hosts)

            for i in range(0, len(chan_list)):
                # Number of streams for instance i (NSTRM)
                n_streams_per_instance = int(addr_list[i][-1])+1
                self.pub_gateway_msg(self.red, chan_list[i], 'NSTRM', n_streams_per_instance, 
                    log, True)
                # Absolute starting channel for instance i (SCHAN)
                s_chan = offset*int(hnchan) + i*n_streams_per_instance*int(hnchan)
                self.pub_gateway_msg(self.red, chan_list[i], 'SCHAN', s_chan, log, True)
                # Destination IP addresses for instance i (DESTIP)
                self.pub_gateway_msg(self.red, chan_list[i], 'DESTIP', addr_list[i], log, True)
        else:
            # If key does not exist, there are no free hosts.
            self.alert(f"No free resources, cannot process data from {product_id}")

    def tracking_start(self, product_id):
        """When a subarray is on source and begins tracking, and the F-engine
           data is trustworthy, this function instructs the processing nodes
           to begin recording data in accordance with the current trigger mode
           settings.

           Before instructing the processing nodes to record, if a list of 
           allowed sources is present, then only those sources are recorded. 
           This list of sources is stored in Redis under the key:
           <array name>:allowed

           If the key <array name>:allowed does not exist, then recordings are
           taken in accordance with the current trigger mode settings.
           
           In addition, the current calibration solutions are retrieved from 
           Telstate and saved both into Redis and as a .npz file. 

           Args:
               product_id (str): name of current subarray. 
        """
        # Check if recording has been enabled: 
        rec_enabled = redis_util.is_rec_enabled(self.red, product_id)
        if rec_enabled:
            # Target information (required here to check list of allowed sources):
            target_str, ra, dec = self.target(product_id)
            # If we have a track which starts without any pointing information, 
            # abort:
            if ra == "UNKNOWN" or dec == "UNKNOWN":
                log.error(f"""Could not retrieve pointing information for 
                            current track, aborting for {product_id}""")
                return
            # Check for list of allowed sources. If the list is not empty, record
            # only if these sources are present.
            # If the list is empty, proceed with recording the current track/scan.
            allowed_key = '{}:allowed'.format(product_id)
            if self.red.exists(allowed_key): # Only this step needed (empty lists don't exist)
                allowed_sources = self.red.lrange(allowed_key, 0, self.red.llen(allowed_key))
                log.info('Filter by the following source names: {}'.format(allowed_sources)) 
                if target_str in allowed_sources:
                    self.record_track(target_str, ra, dec, product_id, n_remaining)
                else:
                    log.info('Target {} not in list of allowed sources, skipping...')
            else:
                log.info('No list of allowed sources, proceeding...')
                self.record_track(target_str, ra, dec, product_id, n_remaining)
        else:
            log.info('Recording disabled, skipping this track.')

    def retrieve_cals(self, product_id):
        """Retrieves calibration solutions and saves them to Redis. They are 
        also formatted and indexed.

        Args:
            product_id (str): the name of the current subarray. 
        """
        # Retrieve and format current telstate endpoint:
        endpoint_key = self.red.get('{}:telstate_sensor'.format(product_id))
        telstate_endpoint = ast.literal_eval(self.red.get(endpoint_key))
        telstate_endpoint = '{}:{}'.format(telstate_endpoint[0], telstate_endpoint[1])
        # Initialise telstate interface object
        self.TelInt = TelstateInterface(self.redis_endpoint, telstate_endpoint) 
        # Before requesting solutions, check if they are newer than the most 
        # recent set that was retrieved. Note that a set is always requested if
        # this is the first recording for a particular subarray configuration.
        # Retrieve last timestamp:
        last_cal_ts = float(self.red.get('coordinator:cal_ts:{}'.format(product_id))) 
        # Retrieve current timestamp:
        current_cal_ts = self.TelInt.get_phaseup_time()
        # Compare:
        if last_cal_ts < current_cal_ts: 
            # Retrieve and save calibration solutions:
            self.TelInt.query_telstate(DIAGNOSTIC_LOC, product_id)
            self.alert("New calibration solutions retrieved.")
            self.red.set('coordinator:cal_ts:{}'.format(product_id), current_cal_ts)
        else:
            self.alert("No calibration solution updates.")

    def record_track(self, target_str, ra_s, dec_s, product_id, n_remaining): 
        """Data recording is initiated by issuing a PKTSTART value to the 
           processing nodes in question via the Hashpipe-Redis gateway [1].
          
           Calibration solutions are retrieved, formatted in the background 
           after a 60 second delay and saved to Redis. This 60 second delay 
           is needed to ensure that the calibration solutions provided by 
           Telstate are current. 

           In addition, other appropriate metadata are published to the 
           processing nodes via the Hashpipe-Redis gateway. 

           Args:
               target_str (str): name of the current source. 
               ra_s (str): RA of the target (sexagesimal form).
               dec_s (str): Dec of the target (sexagesimal form).
               product_id (str): name of current subarray. 
               n_remaining (int): number of remaining recordings to take
               (including the current recording). 
           
           [1] https://arxiv.org/pdf/1906.07391.pdf
        """
 
        # Retrieve calibration solutions after 60 seconds have passed (see above
        # for explanation of this delay):
        delay = threading.Timer(60, lambda:self.retrieve_cals(product_id))
        log.info("Starting delay to retrieve cal solutions in background")
        delay.start()

        # Get list of allocated hosts for this subarray:
        array_key = 'coordinator:allocated_hosts:{}'.format(product_id)
        allocated_hosts = self.red.lrange(array_key, 0, 
            self.red.llen(array_key))

        subarray_group = '{}:{}///set'.format(HPGDOMAIN, product_id)

        # Calculate PKTSTART
        pktstart = self.get_start_idx(allocated_hosts, PKTIDX_MARGIN, log, product_id)
        pktstart_timestamp = redis_util.pktidx_to_timestamp(self.red, pktstart, product_id)
        pktstart_dt = datetime.utcfromtimestamp(pktstart_timestamp)
        pktstart_str = pktstart_dt.strftime("%Y%m%dT%H%M%SZ")
        if abs(pktstart_dt - datetime.utcnow()) > timedelta(minutes=1):
            self.alert(f"bad pktstart: {pktstart_str}")
            return

        sb_id = redis_util.sb_id(self.red, product_id)
        
        # Publish DATADIR to gateway
        datadir = f"/buf0/{pktstart_str}-{sb_id}"
        self.pub_gateway_msg(self.red, subarray_group, 'DATADIR', datadir, 
            log, False)

        # SRC_NAME:
        self.pub_gateway_msg(self.red, subarray_group, 'SRC_NAME', target_str, 
            log, False)
        
        # Publish OBSID to the gateway:
        # OBSID is a unique identifier for a particular observation. 
        obsid = "{}:{}:{}".format(TELESCOPE_NAME, product_id, pktstart_str)
        self.pub_gateway_msg(self.red, subarray_group, 'OBSID', obsid,
            log, False)

        # Set PKTSTART separately after all the above messages have 
        # all been delivered:
        self.pub_gateway_msg(self.red, subarray_group, 'PKTSTART', 
            pktstart, log, False)

        # Set subarray state to 'tracking':
        self.red.set('coordinator:tracking:{}'.format(product_id), '1')

        # Set BLUSE proposal ID flag. If we are in primary time, 
        # decrement nshot if it is > 0.
        if redis_util.is_primary_time(self.red, product_id):
            redis_util.set_last_rec_bluse(self.red, 1)
            nshot = redis_util.get_nshot(self.red, product_id)
            if nshot > 0:
                # Decrement and set:
                redis_util.set_nshot(self.red, product_id, nshot - 1)
                log.info(f"Primary time: nshot now {nshot-1}")
            else:
                log.info(f"Not recording for {product_id} as nshot = {nshot}")
        else:
            redis_util.set_last_rec_bluse(self.red, 0)
        
        # Recording process started:
        self.annotate(
            'RECORD', 
            '{}: Coordinator instructed DAQs to record'.format(product_id)
            )

        # Alert the target selector to the new pointing:
        ra_deg = self.ra_degrees(ra_s)
        dec_deg = self.dec_degrees(dec_s)
        # For the minimal target selector (temporary):
        fecenter = self.centre_freq(product_id) 
        target_information = '{}:{}:{}:{}:{}'.format(obsid, target_str, ra_deg, dec_deg, fecenter)
        self.red.publish(TARGETS_CHANNEL, target_information)

        self.alert(f"Instructed recording for {product_id} to {datadir}")

        # Disable new recording while current recording underway:
        redis_util.disable_recording(self.red, product_id)
        
    def tracking_stop(self, product_id):
        """If the subarray stops tracking a source (more specifically, if the incoming 
           data is no longer to be trusted or used), the following actions are taken:

           - DWELL is set to 0
           - PKTSTART is set to 0
           - DWELL is reset to its original value. 

           This ensures that the processing nodes stop recording (if DWELL has not yet
           already been reached). 

           Args:
               product_id (str): the name of the current subarray.
        """
        tracking_state = self.red.get('coordinator:tracking:{}'.format(product_id))
        # If tracking state transitions from 'track' to any of the other states, 
        # follow the procedure below. Otherwise, do nothing.  
        if tracking_state == '1':
            # Get list of allocated hosts for this subarray:
            array_key = 'coordinator:allocated_hosts:{}'.format(product_id)
            allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
            # Build list of Hashpipe-Redis Gateway channels to publish to:
            chan_list = redis_util.channel_list(HPGDOMAIN, allocated_hosts)

            # NOTE: check how to alter here. 
            #subarray_group = '{}:{}///set'.format(HPGDOMAIN, product_id)

            # Reset DWELL:
            redis_util.reset_dwell(self.red, allocated_hosts, DEFAULT_DWELL)
            self.alert('DWELL has been reset for instances assigned to {}'.format(description))
            
            # Reset tracking state to '0'
            self.red.set('coordinator:tracking:{}'.format(product_id), '0')
            self.alert(f"Tracking stopped for {product_id}")

            
    def deconfigure(self, description):
        """If the current subarray is deconfigured, the following steps are taken:
           
           - For the hosts associated with the current subarray, DESTIP is set to 
             0.0.0.0 - this ensures that they unsubscribe from the multicast group
             and stop receiving raw voltage data from the F-engines. 

           - These hosts are then released back into the pool of available hosts 
             for allocation to other subarrays. 

           Args:
              description (str): the name of the current subarray. 
        """
        # Fetch hosts allocated to this subarray:
        # Note description equivalent to product_id here
        array_key = 'coordinator:allocated_hosts:{}'.format(description)
        allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))

        # Build list of Hashpipe-Redis Gateway channels to publish to:
        chan_list = redis_util.channel_list(HPGDOMAIN, allocated_hosts)

        # Unsubscription process started:
        self.annotate(
            'UNSUBSCRIBE', 
            '{}: Coordinator instructing DAQs to unsubscribe.'.format(description)
            )
        
        # Set DESTIP to 0.0.0.0 individually for robustness. 
        for chan in chan_list:
            self.pub_gateway_msg(self.red, chan, 'DESTIP', '0.0.0.0', log, False)
        self.alert(f"Instructed hosts for {description} to unsubscribe from mcast groups")
        time.sleep(3) # give them a chance to respond

        # Belt and braces restart pipelines
        hostnames_only = [host.split('/')[0] for host in allocated_hosts]
        result = util.restart_pipeline(hostnames_only, 'bluse_hashpipe')
        if len(result) > 0:
            self.alert(f"WARNING: failed to restart bluse_hashpipe on hosts: {result}")
        else:
            self.alert(f"Successfully restarted bluse_hashpipe for dealloacted hosts of {description}")

        # Instruct gateways to leave current subarray group:   
        subarray_group = '{}:{}///set'.format(HPGDOMAIN, description)
        self.red.publish(subarray_group, 'leave={}'.format(description))
        self.alert('Disbanded gateway group: {}'.format(description))

        # Sleep for 20 seconds to allow pipelines to restart:
        time.sleep(20)
        
        # Reset DWELL for all hosts after pipeline restart:
        redis_util.reset_dwell(self.red, allocated_hosts, DEFAULT_DWELL)
        self.alert('DWELL has been reset for instances assigned to {}'.format(description))

        # Release hosts (note: the automator still controls when nshot is set > 0;
        # this ensures recording does not take place while processing is still ongoing).
        # Get list of currently available hosts:
        if self.red.exists('coordinator:free_hosts'):
            free_hosts = self.red.lrange('coordinator:free_hosts', 0,
                self.red.llen('coordinator:free_hosts'))
            self.red.delete('coordinator:free_hosts')
        else:
            free_hosts = []
        # Append released hosts and write 
        # Do this first, since these hosts are free already, and 
        # update keys afterwards. 
        free_hosts = free_hosts + allocated_hosts
        self.red.rpush('coordinator:free_hosts', *free_hosts)    
        # Remove resources from current subarray 
        self.red.delete('coordinator:allocated_hosts:{}'.format(description))
        self.alert("Released {} hosts; {} hosts available".format(len(allocated_hosts),
                                                                len(free_hosts)))
        self.alert('Subarray {} deconfigured'.format(description))

    def data_suspect(self, description, value): 
        """Parse and publish data-suspect mask to the appropriate 
        processing nodes.

        The data-suspect mask provides a global indication of whether or not
        the data from each polarisation from each F-engine can be trusted. A 
        number of parameters are included in this determination (including 
        whether or not a source is being tracked). Please see MeerKAT CAM 
        documentation for further information. 

        Args:
            description (str): the name of the current subarray. 
            value (str): the data-suspect bitmask. 
        """
        bitmask = '#{:x}'.format(int(value, 2))
        # Note description equivalent to product_id here
        # Current Hashpipe-Redis Gateway group name:
        subarray_group = '{}:{}///set'.format(HPGDOMAIN, description)
        # NOTE: Question: do we want to publish the entire bitmask to each 
        # processing node?
        self.pub_gateway_msg(self.red, subarray_group, 'FESTATUS', bitmask, log, False)

    def pointing_update(self, msg_type, description, value):
        """Update pointing information during an observation, and publish 
        results into the Hashpipe-Redis gateway status buffer for the specific
        set of processing nodes allocated to the current subarray. These values 
        include RA, Dec, Az and El and are updated continuously as they change.

        Args:
           msg_type (str): currently indicates the name of the current subarray. 
           (to change in future for consistency). 
           description (str): type of pointing information to update. 
           value (str): the value of the pointing information to update. 
        """
        # NOTE: here, msg_type represents product_id. Need to fix this inconsistent
        # naming convention. 
        # Fetch hosts allocated to this subarray:
        array_key = 'coordinator:allocated_hosts:{}'.format(msg_type)
        allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
        # Hashpipe-Redis gateway group for the current subarray:
        # NOTE: here, msg_type represents product_id.
        subarray_group = '{}:{}///set'.format(HPGDOMAIN, msg_type)
        # RA and Dec (in degrees)
        if 'dec' in description:
            self.pub_gateway_msg(self.red, subarray_group, 'DEC', value, log, False)
            dec_str = self.dec_sexagesimal(value)
            self.pub_gateway_msg(self.red, subarray_group, 'DEC_STR', dec_str, log, False)
        elif 'ra' in description:
            # pos_request_base_ra value is given in hrs (single float
            # value)
            ra_deg = float(value)*15.0 # Convert to degrees
            self.pub_gateway_msg(self.red, subarray_group, 'RA', ra_deg, log, False)
            ra_str = self.ra_sexagesimal(ra_deg)
            self.pub_gateway_msg(self.red, subarray_group, 'RA_STR', ra_str, log, False)
        # Azimuth and elevation (in degrees):
        elif 'azim' in description:
            self.pub_gateway_msg(self.red, subarray_group, 'AZ', value, log, False)
        elif 'elev' in description:
            self.pub_gateway_msg(self.red, subarray_group, 'EL', value, log, False)

    def offset_ut(self, msg_type, value):
        """Publish UT1_UTC, the difference (in seconds) between UT1 and UTC
        (ie, DUT1).

        Args:
           msg_type (str): currently indicates the name of the current subarray. 
           (to change in future for consistency). 
           value (str): the value of UT1 - UTC. 
        """
        # Hashpipe-Redis gateway group for the current subarray:
        # NOTE: here, msg_type represents product_id.
        subarray_group = '{}:{}///set'.format(HPGDOMAIN, msg_type)
        self.pub_gateway_msg(self.red, subarray_group, 'UT1_UTC', value, log, False)

    def get_dwell_time(self, host_key):
        """Get the current dwell time from the status buffer
        stored in Redis for a particular host. 

        Args:
            host_key (str): Key for Redis hash of status buffer
            for a particular host.
        
        Returns:
            dwell_time (int): Dwell time (recording length) in
            seconds.
        """
        dwell_time = 0
        host_status = self.red.hgetall(host_key)
        if len(host_status) > 0:
            if 'DWELL' in host_status:
                dwell_time = host_status['DWELL']
            else:
                log.warning('DWELL is missing for {}'.format(host_key))
        else:
            log.warning('Cannot acquire {}'.format(host_key))
        return dwell_time

    def get_pkt_idx(self, host_key):
        """Get PKTIDX for a host (if active).
        
        Args:
            red_server: Redis server.
            host_key (str): Key for Redis hash of status buffer for a 
            particular active host.
    
        Returns:
            pkt_idx (str): Current packet index (PKTIDX) for a particular 
            active host. Returns None if host is not active.
        """
        pkt_idx = None
        host_status = self.red.hgetall(host_key)
        if len(host_status) > 0:
            if 'NETSTAT' in host_status:
                if host_status['NETSTAT'] != 'idle':
                    if 'PKTIDX' in host_status:
                        pkt_idx = host_status['PKTIDX']
                    else:
                        log.warning('PKTIDX is missing for {}'.format(host_key))
                else:
                    log.warning('NETSTAT is missing for {}'.format(host_key))
        else:
            log.warning('Cannot acquire {}'.format(host_key))
        return pkt_idx

    def get_start_idx(self, host_list, idx_margin, log, product_id):
        """Calculate the packet index at which recording should begin
        (synchronously) for all processing nodes.
    
            Args:
                red_server: Redis server.
                host_list (List): List of host/processing node names (incuding
                instance number).
                idx_margin (int): The safety margin (number of extra packets
                before beginning to record) to ensure a synchronous start across
                processing nodes.
                log: Logger.
            
            Returns:
                start_pkt (int): The packet index at which to begin recording
                data.
        """
        pkt_idxs = []
        for host in host_list:
            host_key = '{}://{}/status'.format(HPGDOMAIN, host)
            pkt_idx = self.get_pkt_idx(host_key)
            if pkt_idx is not None:
                pkt_idxs = pkt_idxs + [pkt_idx]
        if len(pkt_idxs) > 0:
            start_pkt = self.select_pkt_start(pkt_idxs, log, idx_margin, product_id)
            return start_pkt
        else:
            log.warning('No active processing nodes. Setting PKTIDX to 100000 for diagnostic purposes.')
            return 100000

    def select_pkt_start(self, pkt_idxs, log, idx_margin, product_id):
        """Calculates the index of the first packet from which to record
        for each processing node.

        Args:
            pkt_idxs (list): List of the packet indices from each active host.
            log: Logger.
            idx_margin (int): The safety margin (number of extra packets
            before beginning to record) to ensure a synchronous start across
            processing nodes.
        
        Returns:
            start_pkt (int): The packet index at which to begin recording
            data.
        """
        pkt_idxs = np.asarray(pkt_idxs, dtype = np.int64)
        max_idx = redis_util.pktidx_to_timestamp(self.red, np.max(pkt_idxs), product_id)
        med_idx = redis_util.pktidx_to_timestamp(self.red, np.median(pkt_idxs), product_id)
        min_idx = redis_util.pktidx_to_timestamp(self.red, np.min(pkt_idxs), product_id)
        # Error if vary by more than 60 seconds:
        if (max_idx - min_idx) > 60:  
            self.alert(f"PKTIDX varies by >60 seconds for {product_id}")

        pktstart = np.max(pkt_idxs) + idx_margin
        log.info("PKTIDX: Min {}, Median {}, Max {}, PKTSTART {}".format(min_idx, med_idx, max_idx, pktstart))
        return pktstart

    def target(self, product_id):
        """Get target name and coordinates.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              target_str (str): target string including name/description. 
              ra_str (str): RA of current pointing in sexagesimal form.
              dec_str (str): Dec of current pointing in sexagesimal form. 
        """
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.red.lrange(ant_key, 0, self.red.llen(ant_key))
        target_key = "{}:{}_target".format(product_id, ant_list[0])
        target_str = self.get_target(product_id, target_key, 5, 15)
        target_str, ra_str, dec_str = self.target_name(target_str, 16, delimiter = "|")
        return target_str, ra_str, dec_str

    def target_name(self, target_string, length, delimiter = "|"):
        """Limit target description length and replace punctuation with dashes for
        compatibility with filterbank/raw file header requirements. All contents 
        up to the stop character are kept.

        Args:
            target_string (str): Target description string from CBF. A typical 
            example:
            "J0918-1205 | Hyd A | Hydra A | 3C 218 | PKS 0915-11, radec, 
            9:18:05.28, -12:05:48.9"
            length (int): Maximum length for target description.
            delimiter (str): Character at which to split the target string. 
        
        Returns:
            target: Formatted target description suitable for 
            filterbank/raw headers.
            ra_str: RA_STR as accessed from target string.
            dec_str: DEC_STR as accessed from target string.
        """ 
        # Assuming target name or description will always come first
        # Remove any outer single quotes for compatibility:
        target = target_string.strip('\'')
        if 'radec' in target:
            target = target.split(',') # Split by comma separator
            # Check if target name or description present
            if len(target) < 4: 
                target_name = 'NOT_PROVIDED'
                # RA_STR and DEC_STR
                ra_str = target[1].strip()
                dec_str = target[2].strip()
            else:
                target_name = target[0].split(delimiter)[0] # Split at specified delimiter
                target_name = target_name.strip() # Remove leading and trailing whitespace
                target_name = target_name.strip(",") # Remove trailing comma
                # Punctuation below taken from string.punctuation()
                # Note that + and - have been removed (relevant to coordinate names)
                punctuation = "!\"#$%&\'()*,./:;<=>?@[\\]^_`{|}~" 
                # Replace all punctuation with underscores
                table = str.maketrans(punctuation, '_'*30)
                target_name = target_name.translate(table)
                # Limit target string to max allowable in headers (68 chars)
                target_name = target_name[0:length]
                # RA_STR and DEC_STR
                ra_str = target[2].strip()
                dec_str = target[3].strip()
        else:
            # We are unsure of target format since no radec field provided. 
            log.warning("Target name and description incomplete.")
            target_name = "UNKNOWN"
            ra_str = "UNKNOWN"
            dec_str = "UNKNOWN"
        return(target_name, ra_str, dec_str)

    def get_target(self, product_id, target_key, retries, retry_duration):
        """Try to fetch the most recent target name by comparing its timestamp
           to that of the most recent capture-start message.

           Args:
              product_id (str): name of the current subarray. 
              target_key (str): Redis key for the current target. 
              retries (int): number of times to attempt fetching the new 
              target name. 
              retry_duration (float): time (s) to wait between retries. 
           
           Returns:
               target (str): current target name - defaults to 'UNKNOWN' 
               if no new target name is available. 
        """
        for i in range(retries):
            last_target = float(self.red.get("{}:last-target".format(product_id)))
            last_start = float(self.red.get("{}:last-capture-start".format(product_id)))
            if (last_target - last_start) < 0: # Check if new target available
                log.warning("No new target name, retrying.")
                time.sleep(retry_duration)
                continue
            else:
                break
        if i == (retries - 1):
            log.error("No new target name after {} retries; defaulting to UNKNOWN".format(retries))
            target = 'UNKNOWN'
        else:
            target = self.red.get(target_key)
        return target 

    def config(self, cfg_file):
        """Configure the coordinator according to .yml config file.

        Args:
            cfg_file (str): File path for config file (.yml).
        
        Returns:
            List of instances and the number of streams to be processed per 
            instance.
        """
        try:
            with open(cfg_file, 'r') as f:
                try:
                    cfg = yaml.safe_load(f)
                    return(cfg['hashpipe_instances'], 
                        cfg['streams_per_instance'][0])
                except yaml.YAMLError as E:
                    log.error(E)
        except IOError:
            log.error('Config file not found')

    def pub_gateway_msg(self, red_server, chan_name, msg_name, msg_val, logger, write):
        """Format and publish a hashpipe-Redis gateway message. Save messages
        in a Redis hash for later use by reconfig tool. 
        
        Args:
            red_server: Redis server.
            chan_name (str): Name of channel to be published to. 
            msg_name (str): Name of key in status buffer.
            msg_val (str): Value associated with key.
            logger: Logger. 
            write (bool): If true, also write message to Redis database.
        """
        msg = '{}={}'.format(msg_name, msg_val)
        red_server.publish(chan_name, msg)
        # save hash of most recent messages
        if write:
            red_server.hset(chan_name, msg_name, msg_val)
            logger.info('Wrote {} for channel {} to Redis'.format(msg, chan_name))
        logger.info('Published {} to channel {}'.format(msg, chan_name))

    def parse_redis_msg(self, message):
        """Process incoming Redis messages from the various pub/sub channels. 
           Messages are formatted as follows:
           
               <message_type>:<description>:<value>
           
           OR (if there is no associated value): 

               <message_type>:<description>

           If the message does not appear to fit the format, returns an 
           empty string.

           Args:
              message (str): the incoming Redis message. 

           Returns:
              msg_type (str): type of incoming message (eg 'deconfigure')
              description (str): description of incoming message (eg
              'pos_request_base_ra')
              value (str): value associated with incoming message (eg 
              '14:24:32.24'
        """
        msg_type = ''
        description = ''
        value = ''
        msg_parts = message['data'].split(':', 2)
        if len(msg_parts) < 2:
            log.info("Not processing this message: {}".format(message))
        else:
            msg_type = msg_parts[0]
            description = msg_parts[1] 
        if len(msg_parts) > 2:
            value = msg_parts[2]
        return msg_type, description, value        

    def cbf_sensor_name(self, product_id, sensor):
        """Builds the full name of a CBF sensor according to the 
        CAM convention.  
  
        Args:
            product_id (str): Name of the current active subarray.
            sensor (str): Short sensor name (from the .yml config file).
 
        Returns:
            cbf_sensor (str): Full cbf sensor name for querying via KATPortal.
        """
        cbf_name = self.red.get('{}:cbf_name'.format(product_id))
        cbf_prefix = self.red.get('{}:cbf_prefix'.format(product_id))
        cbf_sensor_prefix = '{}:{}_{}_'.format(product_id, cbf_name, cbf_prefix)
        cbf_sensor = cbf_sensor_prefix + sensor
        return cbf_sensor

    def stream_sensor_name(self, product_id, sensor):
        """Builds the full name of a stream sensor according to the 
        CAM convention.  
        
        Args:
            product_id (str): Name of the current active subarray.
            sensor (str): Short sensor name (from the .yml config file).

        Returns:
            stream_sensor (str): Full stream sensor name for querying 
            via KATPortal.
        """
        s_num = product_id[-1] # subarray number
        cbf_prefix = self.red.get('{}:cbf_prefix'.format(product_id))
        stream_sensor = '{}:subarray_{}_streams_{}_{}'.format(product_id, 
            s_num, cbf_prefix, sensor)
        return stream_sensor


    def antennas(self, product_id):
        """Number of antennas, NANTS.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              n_ants (int): the number of antennas in the current subarray. 
        """
        ant_key = '{}:antennas'.format(product_id)
        n_ants = len(self.red.lrange(ant_key, 0, self.red.llen(ant_key)))
        return n_ants
        
    def chan_per_substream(self, product_id):
        """Number of channels per substream - equivalent to HNCHAN.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              n_chans_per_substream (str): the number of channels per substream. 
        """
        sensor_key = self.cbf_sensor_name(product_id, 
                'antenna_channelised_voltage_n_chans_per_substream')   
        n_chans_per_substream = self.red.get(sensor_key)
        return n_chans_per_substream

    def spectra_per_heap(self, product_id):
        """Number of spectra per heap (HNTIME). Please see [1] for further 
           information on the SPEAD protocol.

           [1] Manley, J., Welz, M., Parsons, A., Ratcliffe, S., & 
           Van Rooyen, R. (2010). SPEAD: streaming protocol for exchanging 
           astronomical data. SKA document.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              spectra_per_heap (str): the number of spectra per heap.
        """
        sensor_key = self.cbf_sensor_name(product_id,  
            'tied_array_channelised_voltage_0x_spectra_per_heap')   
        spectra_per_heap = self.red.get(sensor_key)
        return spectra_per_heap

    def coarse_chan_bw(self, product_id, n_freq_chans):
        """Coarse channel bandwidth (from F engines).
           NOTE: no sign information! Equivalent to CHAN_BW.
           
           Args:
              product_id (str): the name of the current subarray.
              n_freq_chans (str): the number of coarse channels

           Returns:
              coarse_chan_bw (float): coarse channel bandwidth. 
        """
        sensor_key = self.cbf_sensor_name(product_id, 
            'adc_sample_rate')
        adc_sample_rate = self.red.get(sensor_key)
        coarse_chan_bw = float(adc_sample_rate)/2.0/int(n_freq_chans)/1e6
        coarse_chan_bw = '{0:.17g}'.format(coarse_chan_bw)
        return coarse_chan_bw

    def centre_freq(self, product_id):
        """Centre frequency (FECENTER).
           
           Args:
              product_id (str): the name of the current subarray.

           Returns:
              centre_freq (float): centre frequency of the current subarray.
        """
        sensor_key = self.stream_sensor_name(product_id,
            'antenna_channelised_voltage_centre_frequency')
        log.info(sensor_key)
        centre_freq = self.red.get(sensor_key)
        log.info(centre_freq)
        centre_freq = float(centre_freq)/1e6
        centre_freq = '{0:.17g}'.format(centre_freq)
        return centre_freq

    def sync_time(self, product_id):
        """Sync time (UNIX, seconds)

           Args:

               product_id (str): the name of the current subarray.

           Returns:

               sync_time (int): the CBF sync time in seconds.             
        """
        sensor_key = self.cbf_sensor_name(product_id, 'sync_time')   
        sync_time = int(float(self.red.get(sensor_key))) # Is there a cleaner way?
        return sync_time

    def samples_per_heap(self, product_id, spectra_per_heap):
        """Equivalent to HCLOCKS.
           
           Args:

               product_id (str): the name of the current subarray.
               spectra_per_heap (str): the number of individual spectra per heap. 
 
           Returns:

               adc_per_heap (int): number of ADC samples per heap. 
       
       """
        sensor_key = self.cbf_sensor_name(product_id,
            'antenna_channelised_voltage_n_samples_between_spectra')
        adc_per_spectra = self.red.get(sensor_key)
        adc_per_heap = int(adc_per_spectra)*int(spectra_per_heap)
        return adc_per_heap

    def ip_offset(self, product_id):
        """Get IP offset (for ingesting fractions of the band)
           
           Args:

               product_id (str): the name of the current subarray.

           Returns:

               offset (int): number of IP addresses to offset by. 
        """
        try:
            offset = int(self.red.get('{}:ip_offset'.format(product_id)))
            if offset > 0:
                log.info('Stream IP offset applied: {}'.format(offset))
        except:
            log.info("No stream IP offset; defaulting to 0")
            offset = 0
        return offset

    def ip_addresses(self, product_id, offset):
        """Acquire and apportion multicast IP groups.
 
           Args:
            
               offset (int): number of IP addresses to offset by. 
               product_id (str): the name of the current subarray.

           Returns:

               addr_list (list): list of SPEAD stream IP address groups.
               port (int): port number.
               n_addrs (int): number of SPEAD IP addresses. 
               n_red_chans (int): number of Redis channels required.
               (corresponding to the number of instances required).   
        """
        all_streams = json.loads(self.json_str_formatter(self.red.get(
            "{}:streams".format(product_id))))
        streams = all_streams[STREAM_TYPE]
        stream_addresses = streams[FENG_TYPE]
        addr_list, port, n_addrs = self.read_spead_addresses(stream_addresses, 
            len(self.hashpipe_instances), 
            self.streams_per_instance, offset)
        n_red_chans = len(addr_list)
        return addr_list, port, n_addrs, n_red_chans

    def json_str_formatter(self, str_dict):
        """Formatting for json.loads
        
        Args:
            str_dict (str): str containing dict of SPEAD streams (received 
            on ?configure).
        
        Returns:
            str_dict (str): str containing dict of SPEAD streams, formatted 
            for use with json.loads
        """
        # Is there a better way of doing this?
        str_dict = str_dict.replace('\'', '"')  # Swap quote types for json format
        str_dict = str_dict.replace('u', '')  # Remove unicode 'u'
        return str_dict

    def read_spead_addresses(self, spead_addrs, n_groups, streams_per_instance, offset):
        """Parses SPEAD addresses given in the format: spead://<ip>+<count>:<port>
        Assumes this format.
        
        Args:
            spead_addrs (str): string containing SPEAD IP addresses in the format 
            above.
            n_groups (int): number of stream addresses to be sent to each 
            processing instance.
            offset (int): number of streams to skip before apportioning IPs.
        
        Returns:
            addr_list (list): list of SPEAD stream IP address groups.
            port (int): port number.
        """
        addrs = spead_addrs.split('/')[-1]
        addrs, port = addrs.split(':')
        try:
            addr0, n_addrs = addrs.split('+')
            n_addrs = int(n_addrs) + 1
            addr_list = self.create_addr_list_filled(addr0, n_groups, n_addrs, 
                streams_per_instance, offset)
        except ValueError:
            addr_list = [addrs + '+0']
            n_addrs = 1
        return addr_list, port, n_addrs

    def create_addr_list_filled(self, addr0, n_groups, n_addrs, streams_per_instance, offset):
        """Creates list of IP multicast subscription address groups.
        Fills the list for each available processing instance 
        sequentially untill all streams have been assigned.
        
        Args:
            addr0 (str): IP address of the first stream.
            n_groups (int): number of available processing instances.
            n_addrs (int): total number of streams to process.
            streams_per_instance (int): number of streams to be processed 
            by each instance.
            offset (int): number of streams to skip before apportioning
            IPs.

        Returns:
            addr_list (list): list of IP address groups for subscription.
        """
        prefix, suffix0 = addr0.rsplit('.', 1)
        suffix0 = int(suffix0) + offset
        n_addrs = n_addrs - offset
        addr_list = []
        if n_addrs > streams_per_instance*n_groups:
            log.warning('Too many streams: {} will not be processed.'.format(
                n_addrs - streams_per_instance*n_groups))
            for i in range(0, n_groups):
                addr_list.append(prefix + '.{}+{}'.format(
                    suffix0, streams_per_instance - 1))
                suffix0 = suffix0 + streams_per_instance
        else:
            n_instances_req = int(np.ceil(n_addrs/float(streams_per_instance)))
            for i in range(1, n_instances_req):
                addr_list.append(prefix + '.{}+{}'.format(suffix0, 
                    streams_per_instance - 1))
                suffix0 = suffix0 + streams_per_instance
            addr_list.append(prefix + '.{}+{}'.format(suffix0, 
                n_addrs - 1 - i*streams_per_instance))
        return addr_list

    def ra_sexagesimal(self, ra):
        """Convert RA from degree form to sexagesimal form.
           Currently displaying 3 decimal places for seconds.

           Args:
               ra (float): right ascension in degree form.

           Returns:
               ra_str (str): right ascension in sexagesimal form.
        """
        h = int(ra//15)
        m = int(ra%15*4)
        s = ra%15*4%1*60
        ra_str = "{:02d}:{:02d}:{:06.3f}".format(h, m, s)
        return ra_str

    def dec_sexagesimal(self, dec):
        """Convert Dec from degree form to sexagesimal form. 
           Currently displaying 3 decimal places for seconds.
 
           Args:
               dec (float): declination in degree form.

           Returns:
               dec (str): declination in sexagesimal form.
        """
        dec = float(dec) # casting to float required
        d = int(dec) 
        m_d = np.abs(dec)%1*60
        m = int(m_d)
        s = m_d%1*60
        dec_str = "{:02d}:{:02d}:{:06.3f}".format(d, m, s)
        return dec_str

    def dec_degrees(self, dec_s):
        """Convert RA from sexagesimal form to degree form. 
        """
        dec = dec_s.split(':')
        d = int(dec[0])
        m = int(dec[1])
        s = float(dec[2])
        if dec[0][0] == '-':
            dec_d = d - m/60.0 - s/3600.0
        else:
            dec_d = d + m/60.0 + s/3600.0
        return dec_d

    def ra_degrees(self, ra_s):
        """Convert RA from sexagesimal form to degree form. 
        """
        ra = ra_s.split(':')
        h = int(ra[0])
        m = int(ra[1])
        s = float(ra[2])
        return h*15 + m*0.25 + s*15.0/3600.0
        
