import json
import numpy as np
import time

from coordinator import util, redis_util
from coordinator.logger import log

FENG_TYPE = "wide.antenna-channelised-voltage"
STREAM_TYPE = "cbf.antenna_channelised_voltage"
HPGDOMAIN = "bluse"
DEFAULT_DWELL = 290
STREAMS_PER_INSTANCE = 4

def subscribe(r, array, instances, streams_per_instance=STREAMS_PER_INSTANCE):
    """Allocate instances to the appropriate multicast groups
    when a new subarray has been configured. 
    """

    # Configuration process started:
    util.annotate_grafana("CONFIGURE", 
            f"{array}: Coordinator configuring DAQs.")

    # Reset keys:
    r.set(f"coordinator:cal_ts:{array}", 0)

    # Join Hashpipe-Redis gateway group for the current subarray:
    redis_util.create_array_groups(r, instances, array)

    # Apportion multicast groups:
    addr_list, port, n_addrs, n_last = alloc_multicast_groups(r, array, len(instances),
        streams_per_instance)

    # Publish necessary gateway keys:

    # Name of current subarray (SUBARRAY)
    redis_util.set_group_key(r, array, "SUBARRAY", array)

    # Port (BINDPORT)
    redis_util.set_group_key(r, array, "BINDPORT", port)

    # Total number of streams (FENSTRM)
    redis_util.set_group_key(r, array, "FENSTRM", n_addrs)

    # Sync time (UNIX, seconds)
    t_sync = sync_time(r, array)
    redis_util.set_group_key(r, array, "SYNCTIME", t_sync)

    # Centre frequency (FECENTER)
    fecenter = centre_freq(r, array)
    redis_util.set_group_key(r, array, "FECENTER", fecenter)

    # Total number of frequency channels (FENCHAN)
    n_freq_chans = r.get(f"{array}:n_channels")
    redis_util.set_group_key(r, array, "FENCHAN", n_freq_chans)

    # Coarse channel bandwidth (from F engines): CHANBW
    # Note: no sign information!
    chan_bw = coarse_chan_bw(r, array, n_freq_chans)
    redis_util.set_group_key(r, array, "CHAN_BW", chan_bw)

    # Number of channels per substream (HNCHAN)
    hnchan = r.get(cbf_sensor_name(r, array,
            'antenna_channelised_voltage_n_chans_per_substream'))
    redis_util.set_group_key(r, array, "HNCHAN", hnchan)

    # Number of spectra per heap (HNTIME)
    hntime = r.get(cbf_sensor_name(r, array,
            'antenna_channelised_voltage_spectra_per_heap'))
    redis_util.set_group_key(r, array, "HNTIME", hntime)

    # Number of ADC samples per heap (HCLOCKS)
    adc_per_heap = samples_per_heap(r, array, hntime)
    redis_util.set_group_key(r, array, "HCLOCKS", adc_per_heap)

    # Number of antennas (NANTS)
    nants = r.llen(f"{array}:antennas")
    redis_util.set_group_key(r, array, "NANTS", nants)

    # Set DWELL to 0 on configure
    redis_util.set_group_key(r, array, "DWELL", 0)

    # Make sure PKTSTART is 0 on configure
    redis_util.set_group_key(r, array, "PKTSTART", 0)

    # SCHAN, NSTRM and DESTIP by instance:
    inst_list = redis_util.sort_instances(list(instances))
    for i in range(len(instances)):
        # Instance channel:
        channel = f"{HPGDOMAIN}://{inst_list[i]}/set"
        # Number of streams for instance i (NSTRM). If this is the final
        # instance on the list, it might not be completely filled.
        if i == len(instances)-1:
            nstrm = n_last + 1
        else:
            nstrm = streams_per_instance
        redis_util.gateway_msg(r, channel, 'NSTRM', nstrm, False)
        # Absolute starting channel for instance i (SCHAN). This is
        # `streams_per_instance` even if the last instance is not completely
        # filled.
        schan = i*streams_per_instance*int(hnchan)
        redis_util.gateway_msg(r, channel, 'SCHAN', schan, False)
        # Destination IP addresses for instance i (DESTIP)
        redis_util.gateway_msg(r, channel, 'DESTIP', addr_list[i], False)

    # Write list of instances for compatibility:
    write_bfr5_instances(r, array, inst_list)

    redis_util.alert(r,
        f":arrow_forward: `{array}` instances subscribed",
        "coordinator")


def unsubscribe(r, array, instances):
    """Ensure that all the specified instances unsubscribe from the
    multicast IP groups.
    """

    # Unsubscription process started:
    util.annotate_grafana("UNSUBSCRIBE",
        f"{array}: Coordinator instructing DAQs to unsubscribe.")

    # Set DESTIP to 0.0.0.0 and DWELL to 0 individually for robustness.
    for instance in instances:
        channel = f"{HPGDOMAIN}://{instance}/set"
        redis_util.gateway_msg(r, channel, "DESTIP", "0.0.0.0", True)
        redis_util.gateway_msg(r, channel, 'DWELL', 0, True)
    time.sleep(3) # give them a chance to respond
    redis_util.alert(r, f":eject: `{array}` unsubscribed", "coordinator")

    # Belt and braces restart DAQs
    result = restart_process(instances, "bluse_hashpipe")
    if len(result) > 0:
        redis_util.alert(r, f":x: `{array}` failed to restart DAQs: {result}",
            "coordinator")
    else:
        redis_util.alert(r, f":repeat: `{array}` restarted DAQs",
            "coordinator")

    # Restart gateways
    result = restart_process(instances, "bluse_redisgw")
    if len(result) > 0:
        redis_util.alert(r, f":x: `{array}` failed to restart gateways: {result}",
            "coordinator")
    else:
        redis_util.alert(r, f":repeat: `{array}` restarted gateways",
            "coordinator")

    # Instruct gateways to leave current subarray group:
    redis_util.destroy_array_groups(r, array)
    log.info(f"Disbanded gateway group: {array}")

    # Clear `bfr5_generator` allocated hosts list:
    clear_bfr5_instances(r, array)

    # Sleep for 20 seconds to allow pipelines to restart:
    time.sleep(20)

def restart_process(instances, process):
    """Restart <process> for specified instances.
    Constructs process name based on instance number (appended).
    """
    result = []
    for instance in instances:
        host, n = instance.split("/")
        process_name = f"{process}_{n}"
        log.info(f"Restarting {process_name}")
        if not util.zmq_circus_cmd(host, process_name, "restart"):
            result.append(instance)
    return result

def samples_per_heap(r, array, spectra_per_heap):
    """Equivalent to HCLOCKS.
    """
    sensor_key = cbf_sensor_name(r, array,
        'antenna_channelised_voltage_n_samples_between_spectra')
    adc_per_spectra = r.get(sensor_key)
    adc_per_heap = int(adc_per_spectra)*int(spectra_per_heap)
    return adc_per_heap

def coarse_chan_bw(r, array, n_freq_chans):
    """Coarse channel bandwidth (from F engines). Formatted for Hashpipe-Redis
    gateway.
    NOTE: no sign information! Equivalent to CHAN_BW.
    """
    sensor_key = cbf_sensor_name(r, array, 'adc_sample_rate')
    adc_sample_rate = r.get(sensor_key)
    coarse_chan_bw = float(adc_sample_rate)/2.0/int(n_freq_chans)/1e6
    coarse_chan_bw = '{0:.17g}'.format(coarse_chan_bw)
    return coarse_chan_bw

def centre_freq(r, array):
    """Acquire the current centre frequency (FECENTER). Format for use with
    the Hashpipe-Redis gateway.
    """
    sensor_key = stream_sensor_name(r, array,
        'antenna_channelised_voltage_centre_frequency')
    centre_freq = r.get(sensor_key)
    centre_freq = float(centre_freq)/1e6
    centre_freq = '{0:.17g}'.format(centre_freq)
    return centre_freq

def sync_time(r, array):
    """Retrieve the current sync time.
    """
    sensor_key = cbf_sensor_name(r, array, 'sync_time')
    sync_time = int(float(r.get(sensor_key))) # Is there a cleaner way?
    return sync_time

def stream_sensor_name(r, array, sensor):
    """Builds full name of a stream sensor according to the CAM convention.
    """
    arr_num = array[-1] # subarray number
    cbf_prefix = r.get(f"{array}:cbf_prefix")
    return f"{array}:subarray_{arr_num}_streams_{cbf_prefix}_{sensor}"

def cbf_sensor_name(r, array, sensor):
    """Builds the full name of a CBF sensor according to the CAM convention.
    """
    cbf_name = r.get(f"{array}:cbf_name")
    cbf_prefix = r.get(f"{array}:cbf_prefix")
    cbf_sensor = f"{array}:{cbf_name}_{cbf_prefix}_{sensor}"
    return cbf_sensor

def num_requested(r, array, streams_per_instance=STREAMS_PER_INSTANCE):
    """Return the number of DAQ instances that would be sufficient to process
    the full bandwidth for the current subarray.
    """
    # Get dictionary of all stream data.
    stream_data = r.get(f"{array}:streams")

    # format for json:
    stream_data = stream_data.replace('\'', '"')
    stream_data = stream_data.replace('u', '')
    stream_data = json.loads(stream_data)
    stream_addresses = stream_data[STREAM_TYPE][FENG_TYPE]

    # Address format: spead://<ip>+<count>:<port>
    addrs = stream_addresses.split('/')[-1]
    addrs, port = addrs.split(':')
    addr0, n_addrs = addrs.split('+')
    total_addrs = int(n_addrs) + 1 # Total number of addresses
    return int(np.ceil(total_addrs/float(streams_per_instance)))

def alloc_multicast_groups(r, array, n_instances,
                           streams_per_instance=STREAMS_PER_INSTANCE):
    """Apportion multicast groups evenly among specified instances.
    """

    # Get dictionary of all stream data.
    stream_data = r.get(f"{array}:streams")

    # format for json:
    stream_data = stream_data.replace('\'', '"')
    stream_data = stream_data.replace('u', '')
    stream_data = json.loads(stream_data)
    stream_addresses = stream_data[STREAM_TYPE][FENG_TYPE]

    # Address format: spead://<ip>+<count>:<port>
    addrs = stream_addresses.split('/')[-1]
    addrs, port = addrs.split(':')
    last_added = 0
    try:
        addr0, n_addrs = addrs.split('+')
        n_addrs = int(n_addrs) + 1 # Total number of addresses

        # allocate, filling in sequence:
        first_octets, last_octet = addr0.rsplit(".", 1)
        last_octet = int(last_octet)
        addr_list = []
        if n_addrs > streams_per_instance*n_instances:
            extra = n_addrs - streams_per_instance*n_instances
            log.warning(f"Too many streams: {extra} will not be processed.")
            for i in range(0, n_instances):
                allocated = f".{last_octet}+{streams_per_instance - 1}"
                addr_list.append(first_octets + allocated)
                last_octet = last_octet + streams_per_instance
        else:
            n_required = int(np.ceil(n_addrs/float(streams_per_instance)))
            for i in range(1, n_required):
                allocated = f".{last_octet}+{streams_per_instance - 1}"
                addr_list.append(first_octets + allocated)
                last_octet = last_octet + streams_per_instance
            last_added = n_addrs - 1 - (n_required-1)*streams_per_instance
            final = f".{last_octet}+{last_added}"
            addr_list.append(first_octets + final)

    # If there is only one address:
    except ValueError:
        addr_list = [addrs + '+0']
    return addr_list, port, n_addrs, last_added

def write_bfr5_instances(r, array, instances):
    """Compatibility function to alert the `bfr5_generator` to the current
    list of active hosts for which bfr5 files should be generated.

    `instances` should be a list of strings.
    """
    key = f"coordinator:allocated_hosts:{array}"
    # clear if old list still exists
    if r.exists(key):
        r.delete(key)
    r.rpush(key, *instances)

def clear_bfr5_instances(r, array):
    """Compatibility function to clear the current list of active hosts for
    the `bfr5_generator`.
    """
    key = f"coordinator:allocated_hosts:{array}"
    if r.exists(key):
        r.delete(key)
