#!/usr/bin/python

import ala
import db
import logging

if __name__ == '__main__':
    """
    Does a full occurence record import of every species in the 'species'
    table using data form ALA.  Doesn't delete existing occurrence records.
    """

    for species in db.species.select().execute().fetchall():
        lsid = ala.lsid_for_species_scientific_name(species['name'])
        if not lsid:
            logging.warning("Can't find any LSID for species: " +
                species['name'])
            continue

        logging.info('Getting records for %s (%s)', species['name'], lsid)
        num_records = 0
        for record in ala.records_for_species(lsid, 'search'):
            num_records += 1
            db.occurrences.insert().execute(
                latitude=record.latitude,
                longitude=record.longitude,
                species_id=species['id']
            )

        if num_records == 0:
            logging.warning('Found 0 records for %s', species['name'])
