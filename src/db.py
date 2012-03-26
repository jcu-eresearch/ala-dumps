#!/usr/bin/python

import sys
from sqlalchemy import \
    create_engine, MetaData, Table, Column, ForeignKey, INT, \
    PrimaryKeyConstraint, Index
from sqlalchemy.dialects.mysql import \
    SMALLINT, TINYINT, ENUM, VARCHAR, DATETIME, FLOAT, BINARY, TEXT

engine = create_engine('mysql://ap03:ap03@localhost/ap03')
metadata = MetaData()
metadata.bind = engine

species = Table('species', metadata,
    Column('id', SMALLINT(unsigned=True), primary_key=True),
    Column('scientific_name', VARCHAR(256), nullable=False),
    Column('common_name', VARCHAR(256), nullable=False)
)

sources = Table('sources', metadata,
    Column('id', TINYINT(unsigned=True), primary_key=True),
    Column('name', VARCHAR(256), nullable=False),
    Column('last_import_time', DATETIME(), nullable=True)
)

occurrences = Table('occurrences', metadata,
    Column('id', INT(unsigned=True), primary_key=True),
    Column('latitude', FLOAT(), nullable=False),
    Column('longitude', FLOAT(), nullable=False),
    Column('rating', ENUM('good', 'suspect', 'bad'), nullable=False),
    Column('species_id', SMALLINT(unsigned=True), ForeignKey('species.id'),
        nullable=False),
    Column('source_id', TINYINT(unsigned=True), ForeignKey('sources.id'),
        nullable=False),
    Column('source_record_id', BINARY(16), nullable=True),

    Index('idx_species_id', 'species_id'),

    mysql_engine='MyISAM'
)

users = Table('users', metadata,
    Column('id', INT(unsigned=True), primary_key=True),
    Column('email', VARCHAR(256), nullable=False)
)

ratings = Table('ratings', metadata,
    Column('id', INT(unsigned=True), primary_key=True),
    Column('user_id', INT(unsigned=True), ForeignKey('users.id'),
        nullable=False),
    Column('comment', TEXT(), nullable=False),
    Column('rating', ENUM('good', 'suspect', 'bad'), nullable=False)
)

occurrences_ratings_bridge = Table('occurrences_ratings_bridge', metadata,
    Column('occurrence_id', INT(unsigned=True), nullable=False),
    Column('rating_id', INT(unsigned=True), nullable=False),

    PrimaryKeyConstraint('occurrence_id', 'rating_id')
)


if __name__ == '__main__':
    if '--debug-populate' in sys.argv:
        # This is an old name for 'Cracticus tibicen'. Previously they were
        # thought to be two separate species, but they are now all classified
        # as 'Cracticus tibicen'.
        #
        # WARNING: ALA has about 350,000 occurrences for this species
        species.insert().execute(
            scientific_name='Gymnorhina tibicen',
            common_name='Australian Magpie')

        species.insert().execute(
            scientific_name='Motacilla flava',
            common_name='Yellow Wagtail')

        species.insert().execute(
            scientific_name='Ninox strenua',
            common_name='Powerful Owl')

        species.insert().execute(
            scientific_name='Falco hypoleucos',
            common_name='Grey Falcon')
