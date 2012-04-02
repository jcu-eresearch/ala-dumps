#!/usr/bin/env python

import pathfix
import ala
import logging

if __name__ == '__main__':
    logging.basicConfig()
    logging.root.setLevel(logging.INFO)
    for species in ala.all_bird_species():
        print species
