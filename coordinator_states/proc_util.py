
def get_unprocessed(r, name):
    """Return the set of unprocessed directories for the current
    subarray. 
    """
    unprocessed = {}
    datadir = r.rpop(f"{name}:unprocessed")
    while datadir:
        unprocessed.add(datadir)
        datadir = r.rpop(f"{name}:unprocessed")
    return unprocessed

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