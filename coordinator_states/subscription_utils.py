import json
import numpy as np
import time

from automator import util, redis_util
from automator.logger import log

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
            f"{product_id}: Coordinator configuring DAQs.")
    redis_util.alert(r, "Subscribing to new multicast groups", "coordinator")

    # Reset keys:
    r.set(f"coordinator:cal_ts:{array}", 0)

    # Join Hashpipe-Redis gateway group for the current subarray:
    redis_util.join_gateway_group(r, instances, array, HPGDOMAIN)
    array_group = f"{HPGDOMAIN}:array:///set"

    # Apportion multicast groups:
    addr_list, port, n_addrs, n_last = alloc_multicast_groups(r, array, len(instances),
        streams_per_instance)

    # Publish necessary gateway keys:

    # Name of current subarray (SUBARRAY)
    redis_util.gateway_msg(r, array_group, 'SUBARRAY', array, False)

    # Port (BINDPORT)
    redis_util.gateway_msg(r, array_group, 'BINDPORT', port, False)

    # Total number of streams (FENSTRM)
    redis_util.gateway_msg(r, array_group, 'FENSTRM', n_addrs, False)

    # Sync time (UNIX, seconds)
    t_sync = sync_time(r, array)
    redis_util.gateway_msg(r, array_group, 'SYNCTIME', t_sync, False)

    # Centre frequency (FECENTER)
    fecenter = centre_freq(r, array)
    redis_util.gateway_msg(r, array_group, 'FECENTER', fecenter, False)

    # Total number of frequency channels (FENCHAN)
    n_freq_chans = r.get(f"{array}:n_channels")
    redis_util.gateway_msg(r, array_group, 'FENCHAN', n_freq_chans, False)

    # Coarse channel bandwidth (from F engines): CHANBW
    # Note: no sign information!
    chan_bw = coarse_chan_bw(r, array, n_freq_chans)
    redis_util.gateway_msg(r, array_group, 'CHAN_BW', chan_bw, False)

    # Number of channels per substream (HNCHAN)
    hnchan = r.get(cbf_sensor_name(r, array,
            'antenna_channelised_voltage_n_chans_per_substream'))
    redis_util.gateway_msg(r, array_group, 'HNCHAN', hnchan, False)

    # Number of spectra per heap (HNTIME)
    hntime = r.get(cbf_sensor_name(r, array,
            'tied_array_channelised_voltage_0x_spectra_per_heap'))
    redis_util.gateway_msg(r, array_group, 'HNTIME', hntime, False)

    # Number of ADC samples per heap (HCLOCKS)
    adc_per_heap = samples_per_heap(r, array, hntime)
    redis_util.gateway_msg(r, array_group, 'HCLOCKS', adc_per_heap, False)

    # Number of antennas (NANTS)
    nants = r.llen(f"{array}:antennas")
    redis_util.gateway_msg(r, array_group, 'NANTS', nants, False)

    # Make sure PKTSTART is 0 on configure
    redis_util.gateway_msg(r, array_group, 'PKTSTART', 0, False)

    # SCHAN, NSTRM and DESTIP by instance:
    for i in range(len(instances)):
        # Number of streams for instance i (NSTRM)
        if i == len(instances)-1:
            nstrm = n_last
        else:
            nstrm = streams_per_instance
        redis_util.gateway_msg(r, array_group, 'NSTRM', nstrm, False)
        # Absolute starting channel for instance i (SCHAN)
        schan = i*nstrm*int(hnchan)
        redis_util.gateway_msg(r, array_group, 'SCHAN', schan, False)
        # Destination IP addresses for instance i (DESTIP)
        redis_util.gateway_msg(r, array_group, 'DESTIP', addr_list[i], False)

    redis_util.alert(r, f"Subscribed: {array}", "coordinator")


def unsubscribe(r, array, instances):
    """Ensure that all the specified instances unsubscribe from the
    multicast IP groups.
    """

    # Unsubscription process started:
    self.annotate("UNSUBSCRIBE",
        f"{description}: Coordinator instructing DAQs to unsubscribe.")

    # Set DESTIP to 0.0.0.0 individually for robustness.
    for instance in instances:
        channel = f"{HPGDOMAIN}://{instance}/set"
        redis_util.gateway_msg(r, channel, "DESTIP", "0.0.0.0", False)
    redis_util.alert(f"Instructed DAQs for {array} to unsubscribe.")
    time.sleep(3) # give them a chance to respond

    # Belt and braces restart DAQs
    hostnames_only = [instance.split('/')[0] for instance in instances]
    result = util.zmq_circus_cmd(hostnames_only, "bluse_hashpipe", "restart")
    if len(result) > 0:
        redis_util.alert(f"Failed to restart bluse_hashpipe on: {result}")
    else:
        redis_util.alert(f"Restarted bluse_hashpipe on DAQs for {array}")

    # Instruct gateways to leave current subarray group:
    redis_util.leave_gateway_group(r, array, HPGDOMAIN)
    log.info(f"Disbanded gateway group: {array}")

    # Sleep for 20 seconds to allow pipelines to restart:
    time.sleep(20)

    # Reset DWELL for all hosts after pipeline restart:
    redis_util.reset_dwell(r, instances, DEFAULT_DWELL)
    log.info(f"DWELL has been reset for instances assigned to {array}")


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
    sensor_key = self.stream_sensor_name(array,
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