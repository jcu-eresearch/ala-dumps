#!/usr/bin/env python

import sys
import pathfix
import string
import ala
import db
import logging
import binascii
import argparse
import json
from datetime import datetime
from sqlalchemy import func, select

HEX_CHARS = frozenset('1234567890abcdefABCDEF')


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Synchronises local database with ALA')

    parser.add_argument('--dont-add-species', action='store_true',
        dest='dont_add_species', help='''If new species are found in the ALA
        data, then don't add them to the database.''')

    parser.add_argument('--dont-delete-species', action='store_true',
        dest='dont_delete_species', help='''If species in the local database are
        not present in the ALA data, don't delete them. Species can be removed
        from ALA when they are merged into another existing species, or if
        their scientific name changes.''')

    parser.add_argument('--dont-update-occurrences', action='store_true',
        dest='dont_update_occurrences', help='''If this flag is set, doesn't do
        anything to the occurrences table. Useful if you only want to update
        the species table.''')

    parser.add_argument('--doctest', action='store_true', dest='doctest',
            help='''If this flag is set, doctest is run on the module and no
            update is performed.''')

    parser.add_argument('--log-level', type=str, nargs=1,
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default=['INFO'], help='''Determines how much info is printed.''')

    parser.add_argument('config', metavar='config_file', type=str, nargs=1,
            help='''The path to the JSON config file.''')

    return parser.parse_args();


def uuid_hex_to_binary(uuid_hex):
    r'''
    >>> a = uuid_hex_to_binary('8ed2d35f-2911-4c10-ad68-587c96b4686e')
    >>> b = '\x8e\xd2\xd3\x5f\x29\x11\x4c\x10\xad\x68\x58\x7c\x96\xb4\x68\x6e'
    >>> a == b
    True
    '''
    uuid_hex = ''.join(c for c in uuid_hex if c in HEX_CHARS)
    assert len(uuid_hex) == 32
    return binascii.unhexlify(uuid_hex)


def get_existing_species_from_db():
    '''Returns a dict mapping scientific name to a db row dict'''
    all_species = {}
    for row in db.species.select().execute():
        all_species[row['scientific_name']] = row;
    return all_species

def get_all_ala_species():
    '''Returns a dict mapping scientific name to ala.Species object'''
    all_species = {}
    for result in ala.all_bird_species():
        all_species[result.scientific_name] = result
    return all_species

def update_species(add_new=True, delete_old=True):
    '''Updates the species table in the database

    Checks ALA for new species, and species that have been deleted (e.g. merged
    into another existing species).
    '''
    if not add_new and not delete_old:
        return

    logging.debug('Getting list of species from db...')
    local = get_existing_species_from_db()
    local_set = frozenset(local.iterkeys())

    logging.debug('Getting list of species from ALA...')
    remote = get_all_ala_species()
    remote_set = frozenset(remote.iterkeys())

    if delete_old:
        deleted_species = local_set - remote_set
        for name, row in local.iteritems():
            if name in deleted_species:
                delete_species(row)

    if add_new:
        added_species = remote_set - local_set
        for name, species in remote.iteritems():
            if name in added_species:
                add_species(species)


def add_species(species):
    '''species must be an ala.Species object'''
    logging.info('Adding new species "%s"', species.scientific_name)
    db.species.insert().execute(
        scientific_name=species.scientific_name,
        common_name=species.common_name)


def delete_species(row):
    '''species must be a row dict from the db'''
    logging.info('Deleting species "%s"', row['scientific_name'])
    db.species.delete().where(db.species.c.id == row['id']).execute()


def update_occurrences(from_d, to_d, ala_source_id):
    '''Updates the occurrences table of the db with data from ALA

    Will use whatever is in the species table of the database, so call
    update_species before this function.
    '''

    for row in db.species.select().execute():
        species = ala.species_for_scientific_name(row['scientific_name'])
        if species is None:
            logging.WARNING('Species not found at ALA: %s',
                species.scientific_name)
            continue

        logging.info('Getting records for %s', species.scientific_name)

        num_records = 0
        for record in ala.records_for_species(species.lsid, 'search', from_d, to_d):
            num_records += 1
            update_occurrence(record, ala_source_id, row['id'])

        if num_records == 0:
            logging.warning('Found 0 records for %s', species.scientific_name)


def update_occurrence(occurrence, ala_source_id, species_id):
    '''Adds/modifies a single record'''
    record_id_binary = uuid_hex_to_binary(occurrence.uuid)
    s = select([func.count('*')],
        # where
        (db.occurrences.c.source_id == ala_source_id)
        & (db.occurrences.c.source_record_id == record_id_binary)
    )
    already_exists = db.engine.execute(s).scalar() > 0
    if already_exists:
        db.occurrences.update()\
            .where(source_id=ala_source_id)\
            .where(source_record_id=record_id_binary)\
            .execute(
                latitude=occurrence.latitude,
                longitude=occurrence.longitude,
                rating='assumed valid', # TODO: determine rating
                species_id=species_id
            )
    else:
        db.occurrences.insert().execute(
            latitude=occurrence.latitude,
            longitude=occurrence.longitude,
            rating='assumed valid',  # TODO: determine rating
            species_id=species_id,
            source_id=ala_source_id,
            source_record_id=record_id_binary
        )

def update():
    ala_source = db.sources.select().execute(name='ALA').fetchone()
    from_d = ala_source['last_import_time']
    to_d = datetime.utcnow()

    update_species(not args.dont_add_species, not args.dont_delete_species)

    if not args.dont_update_occurrences:
        update_occurrences(from_d, to_d, ala_source['id'])
        # only set the last_import_time if records were updated
        db.sources.update().\
                where(db.sources.c.id == ala_source['id']).\
                values(last_import_time=to_d).\
                execute()

    logging.info("Finished at %s", str(datetime.utcnow()))


if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig()
    logging.root.setLevel(logging.__dict__[args.log_level[0]])

    if args.doctest:
        print 'Doctesting...'
        import doctest
        doctest.testmod()
        sys.exit()

    with open(args.config[0], 'rb') as f:
        db.connect(json.load(f))

    logging.info("Started at %s", str(datetime.now()))
    try:
        update()
    finally:
        logging.info("Ended at %s", str(datetime.now()))


