"""
Inspect files for which no hits were recorded. 
"""

import os
import sys
import argparse

def inspect(directory):
    """Provide a list of dirs for which the .hits file is empty and for which
    there are .h5 intermediate beamformer output files. 
    """
    to_inspect = []
    if os.path.isdir(directory):
        dirs = os.listdir(directory)
        for d in dirs:
            # check beamformer output
            upper = os.path.join(directory, d)
            if not os.path.isdir(f"{upper}/seticore_beamformer"):
                continue
            if not os.listdir(f"{upper}/seticore_beamformer"):
                continue
            # find and check hit file size
            if os.path.isdir(f"{upper}/seticore_search"):
                hits_files = [f for f in os.listdir(f"{upper}/seticore_search") if f.endswith("hits")]
                for hits_file in hits_files:
                    if os.path.getsize(f"{upper}/seticore_search/{hits_file}") == 0:
                        to_inspect.append(d)
        return to_inspect 
    else:
        print("Not a directory.")

def cli(args = sys.argv[0]):
    """CLI for script to inspect hits files and associated .h5 files. 
    """
    usage = f"{args} [options]"
    description = "Inspect hits files."
    parser = argparse.ArgumentParser(usage = usage,
                                     description = description)
    parser.add_argument("-d",
                        "--directory",
                        type = str,
                        default = "/scratch/data",
                        help = "Top level directory containing output files.")
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    print(inspect(directory = args.directory))


if __name__ == "__main__":
    cli()