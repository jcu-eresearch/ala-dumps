#!/usr/bin/env python

import sys
import pathfix
import string
import ala
import db
import logging
import binascii
import argparse
from datetime import datetime

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

    return parser.parse_args();


def all_species_with_lsids():
    for species in db.species.select().execute().fetchall():
        scientific_name = species['scientific_name']
        lsid = ala.lsid_for_species_scientific_name(scientific_name)
        if lsid is not None:
            yield species, lsid
        else:
            logging.warning("Can't find any LSID for species: " +
                scientific_name)


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
    for species in db.species.select().execute():
        all_species[species['scientific_name']] = species;
    return all_species

def get_all_ala_species():
    '''Returns a dict mapping scientific name to ala.Species object'''
    all_species = {}
    for species in ala.all_bird_species():
        all_species[species.scientific_name] = species
    return all_species

def update_species(add_new=True, delete_old=True):
    '''Updates the species table in the database

    Checks ALA for new species, and species that have been deleted (e.g. merged
    into another existing species).
    '''
    logging.info('Getting list of species from db...')
    local = get_existing_species_from_db()
    local_set = set(local.iterkeys())

    logging.info('Getting list of species from ALA...')
    remote = get_all_ala_species()
    remote_set = set(remote.iterkeys())

    if delete_old:
        logging.info('Deleting species not found at ALA...')
        deleted_species = local_set - remote_set
        delete_species_from_db(
            [s for name, s in local.iteritems() if name in deleted_species])

    if add_new:
        logging.info('Adding new species found at ALA...')
        added_species = remote_set - local_set
        add_species_to_db(
            [s for name, s in remote.iteritems() if name in added_species])


def add_species_to_db(species):
    '''species must be an iterable of ala.Species objects'''
    for s in species:
        # TODO: here
        logging.info('Adding new species "%s"', s.scientific_name)


def delete_species_from_db(species):
    '''species must be an iterable of row dicts from the species db table'''
    for s in  species:
        # TODO: here
        logging.info('Deleting species "%s"', s['scientific_name'])

def update_occurrences():
    '''Updates the occurrences table of the db with data from ALA

    Will use whatever is in the species table of the database, so call
    update_all_species_in_db before this function.
    '''
    return  # TODO: do here properly

    for species, lsid in all_species_with_lsids():
        logging.info('Getting records for %s (%s)', species['common_name'],
                lsid)

        num_records = 0
        for record in ala.records_for_species(lsid, 'search', from_d, to_d):
            num_records += 1
            db.occurrences.insert().execute(
                latitude=record.latitude,
                longitude=record.longitude,
                rating='good',  # TODO: determine rating
                species_id=species['id'],
                source_id=ala_source['id'],
                source_record_id=uuid_hex_to_binary(record.uuid)
            )

        if num_records == 0:
            logging.warning('Found 0 records for %s', species['common_name'])


if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig()
    logging.root.setLevel(logging.__dict__[args.log_level[0]])

    if args.doctest:
        print 'Doctesting...'
        import doctest
        doctest.testmod()
        sys.exit()


    ala_source = db.sources.select().execute(name='ALA').fetchone()
    from_d = ala_source['last_import_time']
    to_d = datetime.utcnow()

    update_species(not args.dont_add_species, not args.dont_delete_species)

    if not args.dont_update_occurrences:
        update_occurences()

    db.sources.update().\
            where(db.sources.c.id == ala_source['id']).\
            values(last_import_time=to_d).\
            execute()
