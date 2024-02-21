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
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import healpy as hp
from datetime import datetime, timedelta

NSIDE = 64
NPIX = hp.nside2npix(NSIDE)

def cli(args = sys.argv[0]):
    usage = "{} [options]".format(args)
    description = "Analyse prior observations by crawling recordings."
    parser = argparse.ArgumentParser(prog = "obs-stat",
                                     usage = usage,
                                     description = description)
    parser.add_argument("-d",
                        type = str,
                        default = None,
                        help = 'Directory to search.')

    parser.add_argument("-o",
                        type = str,
                        default = "output.csv",
                        help = "Output .csv name.")

    parser.add_argument("-i",
                        type = str,
                        default = "input.csv",
                        help = "Input .csv file.")

    parser.add_argument("-p",
                        action = "store_true",
                        default = False,
                        help = "Plot the sky map.")

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()

    if args.d:
        aggregate(directory = args.d, output = args.o)

    if args.p:
        plot_coverage(args.i)


def aggregate(directory, output):
    """Aggregate recordings by looking at bfr5 files.
    Note, in future, we will use the dedicated BLDW database.
    """
    aggregated = []
    primary_aggregated = []
    file_list = list_bfr5_files(directory)
    for f in file_list:
        srcs, primary = read_bfr5(f)
        aggregated.extend(srcs)
        primary_aggregated.append(primary)
    write_csv(aggregated, output)
    write_csv(primary_aggregated, f"primary_{output}")


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
        decs = f["beaminfo"]["decs"][...]
        ras = f["beaminfo"]["ras"][...]
        fstart = f["obsinfo"]["freq_array"][...][0]*1e3 # in MHz
        nants = f["diminfo"]["nants"][()]

    # get tstart from obsid
    tstart = obsid.split(":")[-1] #last element is timestamp

    # Check if timestamp:
    if not re.match("\d{8}T\d{6}Z", tstart):
        return

    # primary source:
    primary = [srcs[0], ras[0], decs[0], tstart, fstart, nants]

    # format rows
    src_list = []
    for src, ra, dec in zip(srcs, ras, decs):
        src_list.append([src, ra, dec, tstart, fstart, nants])

    return src_list, primary

def write_csv(src_list, file_name):
    """Write csv file of all extracted sources.
    """
    with open(file_name, "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(src_list)

def read_csv(file_name):
    """Read in a .csv file by row.
    """
    srcs = []
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            srcs.append(row)
    return srcs


def plot_coverage(input):
    """Sky coverage plot of locations of observations.
    """
    srcs = read_csv(input)
    timeline(srcs)
    mollweide_plot(srcs)
    healpix_plot(srcs)


def healpix_plot(primary_srcs):
    """healpix plot.
    """
    sky_map = np.zeros(NPIX)
    for src in primary_srcs:
        theta = 0.5*np.pi - float(src[2])
        phi = float(src[1])
        try:
            index = hp.ang2pix(NSIDE, theta, phi)
        except ValueError as e:
            print(f"ValueError, theta = {theta}, phi = {phi}")
        sky_map[index] += 1
    sky_map = sky_map/np.max(sky_map)
    hp.mollview(sky_map, coord=['C'], title='HEALPix Map', unit='numbers', cmap='gray', cbar=False)
    plt.show()
    plt.savefig('map-healpix.png', facecolor='black')

def mollweide_plot(primary_srcs):
    plt.figure(figsize=(10, 5), facecolor="black")
    ax = plt.subplot(111, projection="mollweide")
    ax.set_facecolor("black")
    ax.tick_params(colors="white")
    ax.set_xlabel("RA [deg]", color="white")
    ax.set_ylabel("Dec [deg]", color="white")
    ax.set_xticklabels(["30", "60", "90", "120", "150", "180", "210", "240", "270", "300", "330"])
    plt.title("Stars processed", color="white")
    plt.grid(True, color="gray")
    for src in primary_srcs:
        plt.plot(float(src[1]) - np.pi, float(src[2]), '+', mfc="none", c="cyan", markersize=7)
    plt.savefig('map-mollweide.png', facecolor='black', dpi=600)
    plt.show()

def aitoff_plot(primary_srcs):
    plt.figure(figsize=(10, 5))
    plt.subplot(111, projection="aitoff")
    plt.title("Sky Map")
    plt.grid(True)
    for src in primary_srcs:
        plt.plot(float(src[1]) - 180, float(src[2]), 'o', c="b", markersize=5)
    plt.show()

def dedup(srcs):
    ids = []
    for src in srcs:
        ids.append(src[0])
    ids_set = set(ids)
    return ids_set

def timeline(srcs):

    datetimes = []

    for src in srcs:
        datetimes.append(datetime.strptime(src[3], "%Y%m%dT%H%M%SZ"))

    datetimes.sort()
    cumulative = range(1, len(datetimes) + 1)

    # Format ticks
    def format_y(value, tick_number):
        return f'{int(value / 1000)}k'

    plt.figure(figsize=(10, 6), facecolor='black')
    plt.plot(datetimes, cumulative, marker="o", color='cyan')
    plt.gca().set_facecolor('black')
    plt.gca().tick_params(colors='white', which='both', labelsize='large')
    plt.xlabel('Observing Month', color='white', fontsize=20)
    plt.ylabel('# Stars (Cumulative)', color='white', fontsize=20)
    #plt.title('Observing Progress', color='white', fontsize=20)
    plt.grid(color='gray')

    plt.tick_params(axis='both', which='major', labelsize=18)
    plt.tick_params(axis='both', which='minor', labelsize=18)

    plt.gca().spines['bottom'].set_color('white')
    plt.gca().spines['top'].set_color('white')
    plt.gca().spines['right'].set_color('white')
    plt.gca().spines['left'].set_color('white')

    plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y))

    plt.savefig('observing-progress.png', facecolor='black', dpi=600)

    plt.show()


if __name__ == "__main__":
    cli()
