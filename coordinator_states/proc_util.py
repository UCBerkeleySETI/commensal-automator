
def get_items(r, name, type):
    """Return the set of items of <type> from Redis.
    """
    items = {}
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
    /buf0/<timestamp-part>/blah/blah/etc
    """
    parts = filename.strip("/").split("/")
    if len(parts) < 2:
        return None
    answer = parts[1]
    if not re.match(r"^[0-9]{8}T[0-9]{6}Z-[^/]*$", answer):
        return None
    return answer