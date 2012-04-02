#!/usr/bin/python

import pathfix
import db
import json
import sys

# make sure this isn't run accidentally
if '-h' in sys.argv or len(sys.argv) == 1:
    print "Wipes the database clean and insert some debugging rows."
    print "Don't try use this in production!"
    sys.exit()

# connect
with open('config.example.json', 'rb') as f:
    db.connect(json.load(f))

# wipe
db.species.delete().execute()
db.sources.delete().execute()
db.occurrences.delete().execute()

# insert species
db.species.insert().execute(
    scientific_name='Gymnorhina tibicen',
    common_name='Australian Magpie')

db.species.insert().execute(
    scientific_name='Motacilla flava',
    common_name='Yellow Wagtail')

db.species.insert().execute(
    scientific_name='Ninox (Rhabdoglaux) strenua',
    common_name='Powerful Owl')

db.species.insert().execute(
    scientific_name='Falco (Hierofalco) hypoleucos',
    common_name='Grey Falcon')

# insert ALA source
db.sources.insert().execute(
    name='ALA',
    last_import_time=None)
