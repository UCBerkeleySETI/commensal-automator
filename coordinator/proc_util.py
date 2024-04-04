
import re
import os
import shutil
import numpy as np
import json

from coordinator.logger import log

def check_length(r, datadir, min_duration):
    """Check the length of a recording against threshold.
    """
    key = f"metadata{datadir}"
    meta = r.get(key)
    try:
        meta = json.loads(key)
    except json.decoder.JSONDecodeError:
        log.error("Invalid JSON")
        return
    try:
        t = meta["start_ts"]
        return t
    except KeyError:
        log.error("Missing key: start_ts")
        return
    tend = float(r.get(f"rec_end:{datadir}"))
    if not tend:
        log.error(f"No tend associated with {datadir}")
        return
    if t - tend >= min_duration:
        return True


def completed(r, datadir, n_segments, nbeams, channel):
    """Format and send a message to the target selector instructing it to
    update the observing priority table.
    """
    try:
        meta = r.get(f"metadata:{datadir}")
        meta = json.loads(meta)
    except json.decoder.JSONDecodeError:
        log.error(f"Invalid JSON when reading metadata for {datadir}")
        return
    stop_ts = r.get(f"rec_end:{datadir}")
    if not stop_ts:
        log.error(f"No recording end timestamp for {datadir}")
        return
    t = float(stop_ts) - meta["start_ts"] # in seconds
    update_data = {
        "nsegs":n_segments,
        "band":meta["band"],
        "t":t,
        "nants":meta["nants"],
        "obsid":meta["obsid"],
        "nbeams":nbeams
    }
    msg = f"UPDATE:{json.dumps(update_data)}"
    log.info(f"Publishing update message for {datadir}")
    r.publish(channel, msg)

def datadir_to_obsid(datadir, telescope, array):
    """Convert DATADIR to the corresponding unique obsid.
    """
    #TODO: remove the need for obsid <--> datadir conversions.
    components = re.split("/|-", datadir)
    pktstart_str = components[-2] # last components are pktstart_str, sb_id
    return f"{telescope}:{array}:{pktstart_str}"

def output_summary(codes):
    """Summarise output error codes.
    """
    summary = "codes "
    if codes:
        codes, counts = np.unique(codes, return_counts=True)
        for code, count in zip(codes, counts):
            summary = summary + f"`{code}: {count}` "
    return summary

def get_items(r, name, type):
    """Return the set of items of <type> from Redis.
    """
    items = set()
    item = r.rpop(f"{name}:{type}")
    while item:
        items.add(item)
        item = r.rpop(f"{name}:{type}")
    return items

def increment_n_proc(r):
    """Add 1 to the number of times processing has been run.
    """
    n_proc = get_n_proc(r)
    r.set("automator:n_proc", n_proc + 1)

def get_n_proc(r):
    """Retrieve the absolute number of times processing has been run.
    """
    n_proc = r.get("automator:n_proc")
    if n_proc is None:
        r.set("automator:n_proc", 0)
        return 0
    return int(n_proc)

def timestamped_dir_from_filename(filename):
    """Extracts timestamped section from filenames like:
    `/buf0/<timestamp-part>/blah/blah/etc`
    """
    parts = filename.strip("/").split("/")
    if len(parts) < 2:
        return None
    answer = parts[1]
    if not re.match(r"^[0-9]{8}T[0-9]{6}Z-[^/]*$", answer):
        return None
    return answer

def make_outputdir(outputdir, log):
    """Make an outputdir for seticore search products.
    """
    try:
        os.makedirs(outputdir, mode=0o1777)
        return True
    except FileExistsError:
        log.error("This directory already exists.")
        return False
    except Exception as e:
        log.error(e)
        return False

def rm_datadir(datadir, instance_number, log):
    """Remove directory of RAW recordings after processing. DATADIR is
    expected in the format:
    
    `/buf0/<pktstart timestamp>-<schedule block ID>/...`
    
    Note that `<pktstart timestamp>-<schedule block ID>` is globally unique
    for a directory of raw recordings for the current instance.
    """
    components = datadir.split("/")
    if components[1] != "buf0" and components[1] != "buf1":
        log.error(f"Not a valid datadir: {datadir}")
        return False
    datadir_id = components[2]
    root = f"/buf{instance_number}"
    rm_path = f"{root}/{datadir_id}"
    try:
        shutil.rmtree(rm_path)
        return True
    except Exception as e:
        log.error(e)
        return False