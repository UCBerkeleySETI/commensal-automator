import threading
import numpy as np
import datetime, timedelta

import util
from automator.logger import log

HPGDOMAIN = 'bluse'
# Safety margin for setting index of first packet to record.
PKTIDX_MARGIN = 2048

def record(r, array, instances):
    """Start and check recording for a non-primary time track.

    Calibration solutions are retrieved, formatted in the background
    after a 60 second delay and saved to Redis. This 60 second delay
    is needed to ensure that the calibration solutions provided by
    Telstate are current.
    """

    # Attempt to get current target information:
    target_data = util.retry(3, 1, get_primary_target, r, array, 16, "|")
    if not target_data:
        log.error(f"Could not retrieve current target for {array}")
        return

    # Retrieve calibration solutions after 60 seconds have passed (see above
    # for explanation of this delay):
    delay = threading.Timer(60, lambda:self.retrieve_cals(r, array))
    log.info("Starting delay to retrieve cal solutions in background")
    delay.start()

    # Supply Hashpipe-Redis gateway keys to the instances which will conduct
    # recording:

    # join gateway group:
    array_group = f"{HPGDOMAIN}:{array}///set"

    # Calculate PKTSTART:
    pktstart = get_pktstart(r, instances, PKTIDX_MARGIN, array)
    if not pktstart:
        log.error(f"Could not calculate PKTSTART for {array}")
        return

    # DATADIR
    sb_id = redis_util.sb_id(r, array)
    datadir = f"/buf0/{pktstart["pktstart_str"]}-{sb_id}"
    gateway_msg(r, array_group, 'DATADIR', datadir, False)


def get_primary_target(r, array, length, delimiter = "|"):
    """Attempt to determine the current track's target. 
    
    Belt-and-braces approach:
    Compare target value timestamp with the timestamp of the end
    of the last track. If the target value was last updated during
    the preceding track, it is stale and we should not procede.
    
    Parse target string and extract name, RA and dec. Format target for
    compatibility with filterbank/raw file header requirements. All contents 
    up to the stop character are kept.

    A typical target description string from CBF: 
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
    
    target_val = r.get(f"{array}:target")
    target_ts = float(r.get(f"{array}:last-target")) 
    last_track_end = float(r.get(f"{array}:last-track-end")) 
    if target_ts < last_track_end:
        log.warning(f"No target data yet for current track for {array}.")
        return
    # Assuming target name or description will always come first
    # Remove any outer single quotes for compatibility:
    target = target_val.strip('\'')
    if 'radec' in target:
        target = target.split(',') 
        # Check if target name or description present
        if len(target) < 4: 
            log.warning("Target name not provided.")
            target_name = 'NOT_PROVIDED'
            ra_str = target[1].strip()
            dec_str = target[2].strip()
        else:
            target_name = target[0].split(delimiter)[0] 
            target_name = target_name.strip() 
            target_name = target_name.strip(",") 
            # Note that + and - are not removed 
            punctuation = "!\"#$%&\'()*,./:;<=>?@[\\]^_`{|}~" 
            # Replace all punctuation with underscores
            table = str.maketrans(punctuation, '_'*30)
            target_name = target_name.translate(table)
            # Limit target string to max allowable in headers (68 chars)
            target_name = target_name[0:length]
            ra_str = target[2].strip()
            dec_str = target[3].strip()
        return {"target":target_name, "ra":ra_str, "dec":dec_str}
    else:
        # We are unsure of target format since no radec field provided. 
        log.warning(f"Target name and description incomplete for {array}.")
        return

def get_cals(r, array):
    """Retrieves calibration solutions and saves them to Redis. They are
    also formatted and indexed.
    """
    # Retrieve current telstate endpoint:
    endpoint_key = r.get(f"{array}:telstate_sensor")
    endpoint_val = r.get(endpoint_key)
    # Parse endpoint. Arrives as string in specific format:
    # "('10.98.2.128', 31029)"
    components = endpoint_val.strip("()").split(",")
    telstate_endpoint = f"{components[0]}:{components[1].strip()}"
    # Initialise telstate interface object
    TelInt = TelstateInterface(self.redis_endpoint, telstate_endpoint)

    # Before requesting solutions, check first if they have been delivered
    # since this subarray was last configured:
    last_config_ts = float(r.get(f"{array}:last-config")) # last config ts
    current_cal_ts = TelInt.get_phaseup_time() # current cal ts
    if current_cal_ts < last_config_ts:
        log.warning(f"Calibration solutions not yet available for {array}")
        return

    # Next, check if they are newer than the most recent set that was
    # retrieved. Note that a set is always requested if this is the
    # first recording for a particular subarray configuration.
    last_cal_ts = float(r.get(f"{array}:last-cal"))
    if last_cal_ts < current_cal_ts:
        # Retrieve and save calibration solutions:
        TelInt.query_telstate(array)
        log.info(f"New calibration solutions retrieved for {array}")
        r.set(f"{array}:last-cal", current_cal_ts)
        return "success"
    else:
        self.alert("No calibration solution updates.")


def get_pktstart(r, instances, margin, array):
    """Calculate PKTSTART for specified DAQ instances.
    """

    # Get current packet indices for each instance:
    pkt_indices = []
    for instance in instances:
        key = f"{HPGDOMAIN}://{instance}/status"
        pkt_index = get_pkt_idx(r, key)
        if not pkt_index:
            pkt_indices.append(pkt_index)

    # Calculate PKTSTART
    if len(pkt_indices) > 0:

        pkt_indices = np.asarray(pkt_indices, dtype = np.int64)
        max_index = redis_util.pktidx_to_timestamp(r, np.max(pkt_indices), array)
        med_index = redis_util.pktidx_to_timestamp(r, np.median(pkt_indices), array)
        min_index = redis_util.pktidx_to_timestamp(r, np.min(pkt_indices), array)

        pktstart = np.max(pkt_indices) + margin
        log.info(f"PKTIDX: Min {min_index}, Med {med_index}, Max {max_index}, PKTSTART {pktstart}")

        pktstart_timestamp = redis_util.pktidx_to_timestamp(r, pktstart, array)
        pktstart_dt = datetime.utcfromtimestamp(pktstart_timestamp)
        pktstart_str = pktstart_dt.strftime("%Y%m%dT%H%M%SZ")

        # Check that calculated pktstart is plausible:
        if abs(pktstart_dt - datetime.utcnow()) > timedelta(minutes=1):
            log.error(f"bad pktstart: {pktstart_str} for {array}")
            return

        return {"pktstart":pktstart, "pktstart_str":pktstart_str}
    else:
        log.warning(f"Could not retrieve PKTIDX for {array}")


def get_pkt_idx(r, instance_key):
    """Get PKTIDX for an HPGUPPI_DAQ instance.

    Returns:
        pkt_idx (str): Current packet index (PKTIDX) for a particular
        active host. Returns None if host is not active.
    """
    pkt_idx = None
    # get the status hash from the DAQ instance
    daq_status = r.hgetall(instance_key)
    if len(daq_status) > 0:
        if 'NETSTAT' in daq_status:
            if daq_status['NETSTAT'] != 'idle':
                if 'PKTIDX' in daq_status:
                    pkt_idx = daq_status['PKTIDX']
                else:
                    log.warning(f"PKTIDX is missing for {instance_key}")
        else:
            log.warning(f"NETSTAT is missing for {instance_key}")
    else:
        log.warning(f"Cannot acquire {instance_key}")
    return pkt_idx


def gateway_msg(r, channel, msg_key, msg_val, write):
    """Format and publish a hashpipe-Redis gateway message. Save messages
    in a Redis hash for later use by reconfig tool.

    Args:
        r: Redis server.
        channel (str): Name of channel to be published to.
        msg_key (str): Name of key in status buffer.
        msg_val (str): Value associated with key.
        write (bool): If true, also write message to Redis database.
    """
    msg = f"{msg_key}={msg_val}"
    r.publish(channel, msg)
    log.info(f"Published {msg} to channel {chan_name}")
    # save hash of most recent messages
    if write:
        red_server.hset(channel, msg_key, msg_val)
        log.info(f"Wrote {msg} for channel {chan_name} to Redis")