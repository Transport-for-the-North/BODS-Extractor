# -*- coding: utf-8 -*-
"""Front-end script for running the BODSE AVL package."""

##### IMPORTS #####
import logging

from bodse.avl import avl

##### CONSTANTS #####
LOG = logging.getLogger(__package__)


##### FUNCTIONS #####
def main() -> None:
    # TODO Add command line arguments for config path
    params = avl.DownloaderConfig.load_yaml(avl.CONFIG_PATH)
    avl.main(params)


if __name__ == "__main__":
    main()
