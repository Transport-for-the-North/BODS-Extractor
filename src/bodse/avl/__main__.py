# -*- coding: utf-8 -*-
"""Front-end script for running the BODSE AVL package."""

##### IMPORTS #####

# Built-Ins
import argparse
import enum
import logging
import pathlib

# Local Imports
from bodse.avl import adjust, avl

##### CONSTANTS #####
LOG = logging.getLogger(__package__)


##### FUNCTIONS #####
class Command(enum.Enum):
    """AVL command to run."""

    DOWNLOAD = "download"
    ADJUST = "adjust"

    def __str__(self) -> str:
        return self.value


def _setup_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        __package__,
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "command", choices=list(Command), type=Command, help="AVL command to run"
    )
    parser.add_argument("config", type=pathlib.Path, help="path to config file")

    return parser


def main() -> None:
    parser = _setup_argparser()
    args = parser.parse_args()

    if args.command == Command.DOWNLOAD:
        params = avl.DownloaderConfig.load_yaml(args.config)
        avl.main(params)

    elif args.command == Command.ADJUST:
        params = adjust.AdjustConfig.load_yaml(args.config)
        adjust.main(params)

    else:
        raise ValueError(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
