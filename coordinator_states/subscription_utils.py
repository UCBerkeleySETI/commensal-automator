import json
import numpy as np

from automator import util, redis_util

FENG_TYPE = 'wide.antenna-channelised-voltage'
STREAM_TYPE = 'cbf.antenna_channelised_voltage'

def subscribe(r, array, instances, streams_per_instance):
    """Subscribe the specified instances to the appropriate multicast groups
    when a new subarray has been configured. 
    """
    # Configuration process started:
    util.annotate_grafana("CONFIGURE", 
            f"{product_id}: Coordinator configuring DAQs.")
    redis_util.alert(r, "Subscribing to new multicast groups", "coordinator")
    # Reset keys:
    r.set(f"coordinator:cal_ts:{array}", 0)
    # Apportion multicast groups:
    addr_list, port = alloc_multicast_groups(r, array, len(instances),
        streams_per_instance)


def alloc_multicast_groups(r, array, n_instances, streams_per_instance):
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
        n_addrs = int(n_addrs) + 1

        # allocate evenly:
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
    return addr_list, port