#!/usr/bin/python

import ala
import db
import logging

log = logging.getLogger(__name__)

if __name__ == '__main__':
    """
    Does a full occurence record import of every species in the 'species'
    table using data form ALA.  Doesn't delete existing occurrence records.
    """

    for species in db.species.select().execute().fetchall():
        lsid = ala.lsid_for_species_scientific_name(species['name'])
        log.info('Getting record for %s (%s)', species['name'], lsid)
        for record in ala.records_for_species(lsid, 'search'):
            db.occurrences.insert().execute(
                latitude=record.latitude,
                longitude=record.longitude,
                species_id=species['id']
            )
