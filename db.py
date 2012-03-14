#!/usr/bin/python

import sys
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy import ForeignKey, DateTime, Sequence, Numeric, sql

engine = create_engine('sqlite:///db.sqlite3')
metadata = MetaData()
metadata.bind = engine

species = Table('species', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('created', DateTime, default=sql.functions.now()),
    Column('modified', DateTime, default=sql.functions.now())
)

occurrences = Table('occurrences', metadata,
    Column('id', Integer, primary_key=True),
    Column('species_id', None, ForeignKey('species.id')),
    Column('latitude', Numeric(12,9)),
    Column('longitude', Numeric(12,9)),
    Column('created', DateTime, default=sql.functions.now()),
    Column('modified', DateTime, default=sql.functions.now())
)

if __name__ == '__main__':
    if '--debug-remake' in sys.argv:
        metadata.drop_all()
        metadata.create_all()

    if '--debug-populate' in sys.argv:
        # This is an old name for 'Cracticus tibicen', which is the Australian
        # Mapie. Previously they were thought to be two separate species, but they
        # are now all classified as 'Cracticus tibicen'
        #
        # WARNING: ALA has about 350,000 records for 'Cracticus tibicen'
        species.insert().execute(name='Gymnorhina tibicen')

        # Yellow Wagtail
        species.insert().execute(name='Motacilla flava')

        # Powerful Owl
        species.insert().execute(name='Ninox (Rhabdoglaux) strenua')

        # Grey Falcon
        species.insert().execute(name='Falco (Hierofalco) hypoleucos')
