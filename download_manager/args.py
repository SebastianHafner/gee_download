import argparse


def argument_parser():

    parser = argparse.ArgumentParser(description="Download Args")
    parser.add_argument('-c', "--config-file", dest='config_file', default="", required=True, metavar="FILE",
                        help="path to config file")

    parser.add_argument(
        "opts",
        help="Modify config options using the command-line",
        default=None,
        nargs=argparse.REMAINDER,
    )

    return parser
