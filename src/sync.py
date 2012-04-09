import db
import ala
import logging
import multiprocessing
from sqlalchemy import func, select

log = logging.getLogger(__name__)


class Syncer:

    def __init__(self):
        '''TODO: might pass db into here for unit testing purposes instead of
        using the module directly. Might also do the same for ala module.'''

        row = db.sources.select('id')\
                .where(db.sources.c.name == 'ALA')\
                .execute().fetchone()

        if row is None:
            raise RuntimeError('ALA row missing from sources table in db')

        self.source_row_id = row['id']

    def local_species(self):
        '''Returns all species in the local db in a dict. Scientific name is the
        key, the db row is the value.'''

        species = {}
        for row in db.species.select().execute():
            species[row['scientific_name']] = row;
        return species

    def remote_species(self):
        '''Returns all species available at ALA in a dict. Scientific name is the
        key, the ala.Species object is the value.'''

        species = {}
        for bird in ala.all_bird_species():
            species[bird.scientific_name] = bird
        return species

    def added_and_deleted_species(self):
        '''Returns (added, deleted) where `added` is an iterable of ala.Species
        objects that are not present in the local db, and `deleted` is an iterable
        of rows from the db.species table that were not found at ALA.'''

        local = self.local_species()
        local_set = frozenset(local.keys())
        remote = self.remote_species()
        remote_set = frozenset(remote.keys())

        added_set = remote_set - local_set
        deleted_set = local_set - remote_set

        added = [species for name, species in remote.iteritems() if name in added_set]
        deleted = [row for name, row in local.iteritems() if name in deleted_set]

        return (added, deleted)


    def add_species(self, species):
        '''Adds `species` to the local db, where `s` is an ala.Species
        object'''

        log.info('Adding new species "%s"', species.scientific_name)
        db.species.insert().execute(
            scientific_name=species.scientific_name,
            common_name=species.common_name)

    def delete_species(self, row):
        '''Deletes `row` from the local db, where `s` is a row from the
        db.species table'''

        log.info('Deleting species "%s"', row['scientific_name'])
        db.species.delete().where(db.species.c.id == row['id']).execute()

    def add_or_update_occurrence(self, occurrence, species_id):
        '''Looks up where `occurrence` (an ala.OccurrenceRecord object) already
        exists in the local db. If it does, the db row is updated with the
        information in `occurrence`. If it does not exist, a new row is
        added.

        `species_id` must be supplied as an argument because it is not
        obtainable from `occurrence`'''

        s = select(
            [func.count('*')],
            # where
            (db.occurrences.c.source_id == self.source_row_id)
            & (db.occurrences.c.source_record_id == occurrence.uuid.bytes)
        )

        already_exists = db.engine.execute(s).scalar() > 0

        # TODO: will be faster if I use INSERT ... ON DUPLICATE KEY UPDATE
        #       sql instead of two separate queries
        if already_exists:
            db.occurrences.update()\
                .where(db.occurrences.c.source_id == self.source_row_id)\
                .where(db.occurrences.c.source_record_id == occurrence.uuid.bytes)\
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
                source_id=self.source_row_id,
                source_record_id=occurrence.uuid.bytes)

    def occurrences_changed_since(self, since_date):
        '''Generator for ala.OccurrenceRecord objects.

        Will use whatever is in the species table of the database, so call
        update the species table before calling this function.

        Uses a pool of processes to fetch occurrence records. The subprocesses
        feed the records into a queue which the original process reads and
        yields. This should let the main process access the database at full
        speed while the subprocesses are waiting for more records to arrive
        over the network.'''

        record_q = multiprocessing.Queue()
        pool = multiprocessing.Pool(8, _mp_init, [record_q])
        active_workers = 0

        # fill the pool full with every species
        for row in db.species.select().execute():
            args = (row['scientific_name'], row['id'], since_date)
            pool.apply_async(_mp_fetch, args)
            active_workers += 1

        pool.close()

        # keep reading from the queue until all the subprocesses are finished
        while active_workers > 0:
            record = record_q.get()
            if record is None:
                active_workers -= 1
            else:
                yield record

        # all the subprocesses should be dead by now
        pool.join()


def _mp_init(record_q):
    '''Called when a subprocess is started. See
    Syncer.occurrences_changed_since'''
    _mp_init.record_q = record_q
    _mp_init.log = multiprocessing.log_to_stderr()


def _mp_fetch(species_sname, species_id, since_date):
    '''Gets all relevant records for the given species from ALA, and pumps the
    records into _mp_init.record_q. Puts None into the queue when finished.

    Adds a `species_id` attribute to each species object set to the argument
    given to this function.'''

    species = ala.species_for_scientific_name(species_sname)
    if species is not None:
        num_records = 0
        for record in ala.records_for_species(species.lsid, 'search', since_date):
            record.species_id = species_id
            _mp_init.record_q.put(record)
            num_records += 1

        if num_records > 0:
            _mp_init.log.info('Found %d records for "%s"',
                num_records, species_sname)
    else:
        _mp_init.log.warning('Species not found at ALA: %s', species_sname)

    _mp_init.record_q.put(None)
