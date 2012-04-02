#!/usr/bin/env python

import pathfix
import string
import ala
import db
import logging
import binascii
from datetime import datetime

HEX_CHARS = frozenset('1234567890abcdefABCDEF')


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

def update_all_species_in_db():
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

    logging.info('Deleting species not found at ALA...')
    deleted_species = local_set - remote_set
    delete_species_from_db(
        [s for name, s in local.iteritems() if name in deleted_species])

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

def update_all_occurrences_in_db():
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
    if '--test' in sys.argv:
        import doctest
        doctest.testmod()
        sys.exit()

    logging.root.setLevel(logging.INFO)
    logging.basicConfig()

    ala_source = db.sources.select().execute(name='ala').fetchone()
    from_d = ala_source['last_import_time']
    to_d = datetime.utcnow()

    if '--skip-species-update' not in sys.argv:
        update_all_species_in_db()

    update_all_records_in_db()

    db.sources.update().\
            where(db.sources.c.id == ala_source['id']).\
            values(last_import_time=to_d).\
            execute()
