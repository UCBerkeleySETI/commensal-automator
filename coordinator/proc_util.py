
import re
import os
import shutil

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
    if components[1] != "buf0":
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