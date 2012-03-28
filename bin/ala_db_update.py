#!/usr/bin/env python

import sys
import os.path
srcpath = os.path.realpath(os.path.dirname(__file__) + '/../src')
sys.path.append(srcpath)

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


if __name__ == '__main__':
    if '--test' in sys.argv:
        import doctest
        doctest.testmod()
        sys.exit()

    """
    Does a full occurence record import of every species in the 'species'
    table using data form ALA.  Doesn't delete existing occurrence records.
    """

    logging.root.setLevel(logging.DEBUG)

    ala_source = db.sources.select().execute(name='ala').fetchone()
    from_d = ala_source['last_import_time']
    to_d = datetime.utcnow()

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

    db.sources.update().\
            where(db.sources.c.id == ala_source['id']).\
            values(last_import_time=to_d).\
            execute()
