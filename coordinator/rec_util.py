import threading
import time
import numpy as np
from datetime import datetime, timedelta

from coordinator import util, redis_util
from coordinator.logger import log
from coordinator.telstate_interface import TelstateInterface

HPGDOMAIN = 'bluse'
PKTIDX_MARGIN = 2048 # in packets
TARGETS_CHANNEL = 'target-selector:new-pointing'
DEFAULT_DWELL = 290

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
    delay = threading.Timer(60, lambda:get_cals(r, array))
    log.info("Starting delay to retrieve cal solutions in background")
    delay.start()

    # Supply Hashpipe-Redis gateway keys to the instances which will conduct
    # recording:

    # Gateway group:
    array_group = f"{HPGDOMAIN}:{array}///set"

    # Set DWELL in preparation for recording:
    redis_util.gateway_msg(r, array_group, 'DWELL', DEFAULT_DWELL, True)

    # Calculate PKTSTART:
    pktstart_data = get_pktstart(r, instances, PKTIDX_MARGIN, array)
    if not pktstart_data:
        log.error(f"Could not calculate PKTSTART for {array}")
        return

    # Retrieve fecenter:
    fecenter = centre_freq(r, array)
    if not fecenter:
        log.error(f"Could not retreive FECENTER for {array}")
        return

    # DATADIR
    sb_id = redis_util.sb_id(r, array)
    datadir = f"/buf0/{pktstart_data['pktstart_str']}-{sb_id}"
    redis_util.gateway_msg(r, array_group, 'DATADIR', datadir, False)

    # SRC_NAME:
    redis_util.gateway_msg(r, array_group, 'SRC_NAME', target_data["target"], False)

    # OBSID (unique identifier for a particular observation):
    obsid = f"MeerKAT:{array}:{pktstart_data['pktstart_str']}"
    redis_util.gateway_msg(r, array_group, 'OBSID', obsid, False)

    # Set PKTSTART separately after all the above messages have
    # all been delivered:
    redis_util.gateway_msg(r, array_group, 'PKTSTART', pktstart_data["pktstart"], False)

    # Grafana annotation that recording has started:
    annotate('RECORD', f"{array}: Coordinator instructed DAQs to record")

    # Alert the target selector to the new pointing:
    ra_d = util.ra_degrees(target_data["ra"])
    dec_d = util.dec_degrees(target_data["dec"])
    targets_req = f"{obsid}:{target_data['target']}:{ra_d}:{dec_d}:{fecenter}"
    r.publish(TARGETS_CHANNEL, targets_req)

    # Write datadir to the list of unprocessed directories for this subarray:
    add_unprocessed(r, set(instances), datadir)

    # Start recording timeout timer, with 10 second safety margin:
    rec_timer = threading.Timer(300, lambda:timeout(r, array, "rec_result"))
    log.info("Starting recording timeout timer.")
    rec_timer.start()

    redis_util.alert(r,
        f":black_circle_for_record: Recording `{obsid}` on `{array}`",
        "coordinator")
    # If this is primary time, write datadir to the list of directories to
    # preserve:
    # TODO: check for primary time first
    #if is_primary_time():
    #    add_preserved(r, recording, datadir)

    # Wait half a second to ensure recording has started:
    time.sleep(0.5)
    recording = get_recording(r, instances)

    return set(instances)

def add_unprocessed(r, recording, datadir):
    """Set the list of unprocessed directories.
    """
    log.info(f"Adding {datadir} to <instance>:unprocessed")
    for instance in recording:
        r.lpush(f"{instance}:unprocessed", datadir)


def add_preserved(r, recording, datadir):
    """Set the list of unprocessed directories.
    """
    for instance in recording:
        r.lpush(f"{instance}:preserved", datadir)


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
    try:
        components = endpoint_val.strip("()").split(",")
        ip = components[0].strip("'")
        port = components[1].strip()
        telstate_endpoint = f"{ip}:{port}"
    except:
        log.error(f"Could not parse Telstate endpoint: {endpoint_val}")
        return
    # Initialise telstate interface object
    try:
        TelInt = TelstateInterface(r, telstate_endpoint)
    except Exception as e:
        log.error("Could not connect to TelState, details follow:")
        log.error(e)
        return
    # Before requesting solutions, check first if they have been delivered
    # since this subarray was last configured:
    last_config_ts = r.get(f"{array}:last-config") # last config ts
    if not last_config_ts:
        log.warning("No key set for last_config_ts.")
        last_config_ts = 0
    else:
        last_config_ts = float(last_config_ts)
    current_cal_ts = TelInt.get_phaseup_time() # current cal ts
    if current_cal_ts < last_config_ts:
        log.warning(f"Calibration solutions not yet available for {array}")
        return

    # Next, check if they are newer than the most recent set that was
    # retrieved. Note that a set is always requested if this is the
    # first recording for a particular subarray configuration.
    last_cal_ts = r.get(f"{array}:last-cal")
    if not last_cal_ts:
        log.warning("No key set for last_cal_ts.")
        last_cal_ts = 0
    else:
        last_cal_ts = float(last_cal_ts)
    if last_cal_ts < current_cal_ts:
        # Retrieve and save calibration solutions:
        TelInt.query_telstate(array)
        log.info(f"New calibration solutions retrieved for {array}")
        r.set(f"{array}:last-cal", current_cal_ts)
        return "success"
    else:
        log.info("No calibration solution updates")


def get_pktstart(r, instances, margin, array):
    """Calculate PKTSTART for specified DAQ instances.
    """

    log.info(f"instances: {instances}")
    # Get current packet indices for each instance:
    pkt_indices = []
    for instance in instances:
        key = f"{HPGDOMAIN}://{instance}/status"
        pkt_index = get_pkt_idx(r, key)
        if not pkt_index:
            continue
        pkt_indices.append(pkt_index)

    # Calculate PKTSTART
    if len(pkt_indices) > 0:

        pkt_indices = np.asarray(pkt_indices, dtype = np.int64)
        max_ts = redis_util.pktidx_to_timestamp(r, np.max(pkt_indices), array)
        med_ts = redis_util.pktidx_to_timestamp(r, np.median(pkt_indices), array)
        min_ts = redis_util.pktidx_to_timestamp(r, np.min(pkt_indices), array)

        pktstart = np.max(pkt_indices) + margin

        pktstart_timestamp = redis_util.pktidx_to_timestamp(r, pktstart, array)
        pktstart_dt = datetime.utcfromtimestamp(pktstart_timestamp)
        pktstart_str = pktstart_dt.strftime("%Y%m%dT%H%M%SZ")

        log.info(f"PKTIDX: Min {min_ts}, Med {med_ts}, Max {max_ts}, PKTSTART {pktstart_timestamp}")

        # Check that calculated pktstart is plausible:
        if abs(pktstart_dt - datetime.utcnow()) > timedelta(minutes=2):
            log.warning(f"bad pktstart: {pktstart_str} for {array}")
            redis_util.alert(r,
                f":warning: `{array}` bad pktstart",
                "coordinator")
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

def annotate(tag, text):
    response = util.annotate_grafana(tag, text)
    log.info(f"Annotating Grafana, response: {response}")

def centre_freq(r, array):
    """Centre frequency (FECENTER).
    """
    try:
        # build the specific sensor ID for retrieval
        s_num = array[-1] # subarray number
        sensor = "antenna_channelised_voltage_centre_frequency"
        cbf_prefix = r.get(f"{array}:cbf_prefix")
        sensor_key = f"{array}:subarray_{s_num}_streams_{cbf_prefix}_{sensor}"
        centre_freq = r.get(sensor_key)
        centre_freq = float(centre_freq)/1e6
        centre_freq = '{0:.17g}'.format(centre_freq)
        return centre_freq
    except Exception as e:
        log.error(e)


def get_recording(r, instances):
    """Check if given instances are recording.
    """
    ins = redis_util.multiget_by_instance(r, HPGDOMAIN, instances, "DAQSTATE")
    return set([inst[0] for inst in ins if inst[1][0] == "RECORD"])


def timeout(r, array, channel):
    """Temporary timeout mechanism for recordings.
    """
    r.publish(channel, f"rec-timeout:{array}")
