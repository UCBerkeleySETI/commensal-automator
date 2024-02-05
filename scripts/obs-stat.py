"""
Analyse completed observations:
    - Produce a single .csv of all observations: source, timestamp, 
    - skymap
    - skymap heatmap
    - observing over time
"""
import argparse
import sys

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

if __name__ == "__main__":
    cli()