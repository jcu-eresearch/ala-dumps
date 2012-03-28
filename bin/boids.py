#!/usr/bin/env python

import sys
import os.path
srcpath = os.path.realpath(os.path.dirname(__file__) + '/../src')
sys.path.append(srcpath)

import ala
import logging

if __name__ == '__main__':
    logging.basicConfig()
    logging.root.setLevel(logging.INFO)
    for species in ala.all_bird_species():
        species.lsid = None
        print species