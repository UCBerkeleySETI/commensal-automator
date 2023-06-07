from automator import util, redis_util

def subscribe(r, array, instances):
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