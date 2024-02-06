"""
Analyse completed observations:
    - Produce a single .csv of all observations: source, timestamp, 
    - skymap
    - skymap heatmap
    - observing over time
"""
import argparse
import sys
import h5py

def cli(args = sys.argv[0]):
    usage = "{} [options]".format(args)
    description = "Analyse prior observations by crawling recordings."
    parser = argparse.ArgumentParser(prog = "obs-stat",
                                     usage = usage,
                                     description = description)
    parser.add_argument("-d",
                        type = str,
                        default = '.',
                        help = 'Directory to search.')

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    aggregate(directory = args.d)


def aggregate(directory):
    """Aggregate recordings by looking at bfr5 files.
    Note, in future, we will use the dedicated BLDW database.
    """
    pass


def read_bfr5(filename):
    """Open and read selected contents of a specified bfr5 file.
    Returns: sources including:
        - name
        - frequency/band
        - pktstart timestamp
        - number of antennas
    """
    with h5py.File(filename, 'r') as f:
        obsid = f["obsinfo"]["obsid"][()].decode("utf-8")
        src = f["beaminfo"]["src_names"][...].astype(str)
        fstart = f["obsinfo"]["freq_array"][...][0]*1e3 # in MHz
        nants = f["diminfo"]["nants"][...]

    return obsid, fstart, nants, src

if __name__ == "__main__":
    read_bfr5("MeerKAT-array_1-20230907T112709Z.bfr5")