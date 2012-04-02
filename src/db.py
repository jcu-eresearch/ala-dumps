import sys
from sqlalchemy import \
    engine_from_config, MetaData, Table, Column, ForeignKey, INT, \
    PrimaryKeyConstraint, Index
from sqlalchemy.dialects.mysql import \
    SMALLINT, TINYINT, ENUM, VARCHAR, DATETIME, FLOAT, BINARY, TEXT

engine = None
metadata = MetaData()


def connect(engine_config):
    '''Call this before trying to use anything else'''
    try:
        engine = engine_from_config(engine_config, prefix='db.')
        metadata.bind = engine
    except:
        raise RuntimeError('Failed to connect to database. Check the config.')


species = Table('species', metadata,
    Column('id', SMALLINT(unsigned=True), primary_key=True),
    Column('scientific_name', VARCHAR(256), nullable=False),
    Column('common_name', VARCHAR(256), nullable=True),

    mysql_charset='utf8'
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
