import argparse
import sys
import colorama

from metrics import Metrics

VERSION = "0.0.1"


def init_args():
    """Parse and return the arguments."""

    parser = argparse.ArgumentParser(description="Simple Gmail Analyzer")
    parser.add_argument("--top", type=int, default=10, help="Number of results to show")
    parser.add_argument(
        "--user", type=str, default="me", help="User ID to fetch data for"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Verbose output, helpful for debugging"
    )
    parser.add_argument(
        "--version", action="store_true", help="Display version and exit"
    )

    args = vars(parser.parse_args())

    return args


if __name__ == "__main__":
    colorama.init()

    args = init_args()

    if args["version"]:
        print("gmail analyzer v{}".format(VERSION))
        sys.exit()

    Metrics(args).start()
