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
import csv
import re
import os
import glob

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

    parser.add_argument("-o",
                        type = str,
                        default = 'output.csv',
                        help = 'Output directory name')

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    aggregate(directory = args.d, output = args.o)


def aggregate(directory, output):
    """Aggregate recordings by looking at bfr5 files.
    Note, in future, we will use the dedicated BLDW database.
    """
    aggregated = []
    file_list = list_bfr5_files(directory)
    for f in file_list:
        aggregated.append(read_bfr5(f))
    write_csv(aggregated, output)


def list_bfr5_files(directory):
    """"List all bfr5 files in the specified directory for opening.
    """
    return glob.glob(os.path.join(directory, "*bfr5"))


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
        srcs = f["beaminfo"]["src_names"][...].astype(str)
        fstart = f["obsinfo"]["freq_array"][...][0]*1e3 # in MHz
        nants = f["diminfo"]["nants"][()]

    # get tstart from obsid
    tstart = obsid.split(":")[-1] #last element is timestamp

    # Check if timestamp:
    if not re.match("\d{8}T\d{6}Z", tstart):
        return

    # format rows
    src_list = []
    for src in srcs:
        src_list.append([src, tstart, fstart, nants])

    return src_list

def write_csv(src_list, file_name):
    """Write csv file of all extracted sources.
    """
    with open(file_name, "w") as f:
        writer = csv.writer(f)
        writer.writerows(src_list)

if __name__ == "__main__":
    cli()