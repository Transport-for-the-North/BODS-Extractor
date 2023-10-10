# -*- coding: utf-8 -*-
"""Front-end script for running the BODSE AVL package."""

##### IMPORTS #####
import logging

from bodse.avl import avl

##### CONSTANTS #####
LOG = logging.getLogger(__package__)


##### FUNCTIONS #####
def main() -> None:
    avl.main()


if __name__ == "__main__":
    main()
