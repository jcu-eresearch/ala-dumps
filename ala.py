import time
import math
import urllib
import urllib2
import json
import pdb
import tempfile
import os
import os.path
import csv
import shutil
import zipfile
import time


class OccurrenceRecord(object):
    """Plain old data structure for an occurrence record"""

    def __init__(self):
        self.latitude = None
        self.longitude = None
        self.uuid = None
        self.species_lsid = None
        self.species_scientific_name = None

    def __repr__(self):
        return (
            '<record species="{species}" ' +
            'uuid="{uuid}" ' +
            'latLong="{lat}, {lng}" />').format(
                species=self.species_scientific_name,
                uuid=self.uuid,
                lat=self.latitude,
                lng=self.longitude)


def records_for_species(species_lsid, strategy, log):
    """A generator for OccurrenceRecord objects fetched from ALA"""

    if strategy == 'search':
        return _search_records_for_species(species_lsid, log)
    elif strategy == 'download':
        return _downloadzip_records_for_species(species_lsid, log)
    elif strategy == 'facet':
        return _facet_records_for_species(species_lsid, log)
    else:
        raise ValueError('Invalid strategy: ' + strategy)


def _retry(tries=3, delay=2, backoff=2):
    """A decorator that retries a function or method until it succeeds (no
    exception is raised).

    delay sets the initial delay in seconds, and backoff sets the factor by
    which the delay should lengthen after each failure. backoff must be greater
    than 1, or else it isn't really a backoff. tries must be at least 1, and
    delay greater than 0."""

    if backoff <= 1:
        raise ValueError("backoff must be greater than 1")

    tries = math.floor(tries)
    if tries < 1:
        raise ValueError("tries must be >= 1")

    if delay <= 0:
        raise ValueError("delay must be >= 0")

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay

            while True:
                try:
                    return f(*args, **kwargs)
                except:
                    mtries -= 1
                    if mtries > 0:
                        time.sleep(mdelay)
                        mdelay *= backoff
                    else:
                        raise
        return f_retry
    return deco_retry


def _request(url, params=False, use_get=False):
    if params:
        params = urllib.urlencode(params)
        if use_get:
            url += '?' + params
            params = False

    if params:
        return urllib2.urlopen(url, params)
    else:
        return urllib2.urlopen(url)


@_retry()
def _fetch_json(url, params=False, use_get=False):
    """Opens the url and returns the result of urllib2.urlopen"""

    response = _request(url, params, use_get)
    response_str = response.read()
    return_value = json.loads(response_str)
    if len(return_value) == 0:
        raise RuntimeError('ALA returned empty response')
    else:
        return return_value, len(response_str)


@_retry()
def _fetch(url, params=False, use_get=False):
    return _request(url, params, use_get)


def _is_record_json_valid_for_modelling(record):
    #TODO: get list of assertions from Jeremy to check here
    return True


def _chunked_read_and_write(infile, outfile, log):
    chunk_size = 4096
    report_interval = 5.0
    last_report_time = time.time()
    bytes_read = 0
    bytes_read_this_interval = 0

    while True:
        chunk = infile.read(chunk_size)
        if len(chunk) > 0:
            outfile.write(chunk)
            bytes_read += len(chunk)
            bytes_read_this_interval += len(chunk)
        else:
            break

        now = time.time()
        if now - last_report_time > report_interval:
            kbdown = float(bytes_read_this_interval) / 1024.0
            log.info('Read %0.0fkb total (at about %0.2f kb/s)',
                    float(bytes_read) / 1024.0,
                     kbdown / (now - last_report_time))
            last_report_time = now
            bytes_read_this_interval = 0


def _downloadzip_records_for_species(species_lsid, log):
    '''Uses /occurrence/download for maximum speed (gets a single zip file) but
    this is may be problematic due to limits on the number of occurrence
    records downloaded. Was told that the limits currently only apply to fauna,
    so this shouldn't be a problem for animals. If it is a problem, replace
    this function with _ala_search_records_for_species, but it will be slower.
    '''

    file_name = 'data'
    url = 'http://biocache.ala.org.au/ws/occurrences/download'
    params = (
        ('q', 'lsid:' + species_lsid),
        #maybe use a list of specific assertions instead of geospatial_kosher
        ('fq', 'geospatial_kosher:true'),
        #maybe also include basis_of_record:MachineObservation
        ('fq', 'basis_of_record:HumanObservation'),
        ('fields', 'decimalLatitude.p,decimalLongitude.p,scientificName.p'),
        ('email', 'tom.dalling@jcu.edu.au'),
        ('reason', 'testing for AP03 project for JCU'),
        ('file', file_name)
    )

    #need to write zip file to a temp file
    log.info('Requesting zip file from ALA...')
    t = time.time()
    response = _fetch(url, params, True)
    log.info('Response headers received after %0.2f seconds', time.time() - t)

    log.info('Downloading zip file...')
    log.debug('Response headers: %s', dict(response.info()))
    temp_zip_file = tempfile.TemporaryFile()
    t = time.time()
    _chunked_read_and_write(response, temp_zip_file, log)
    t = time.time() - t
    zip_file_size_kb = float(temp_zip_file.tell()) / 1024.0
    log.info('Fetched %0.2fkb zip file in %0.2f seconds (%0.2f kb/s)',
            zip_file_size_kb, t, zip_file_size_kb / t)

    #grab the csv inside
    log.info('Reading csv from zip file...')
    t = time.time()
    zip_file = zipfile.ZipFile(temp_zip_file)
    reader = csv.DictReader(zip_file.open(file_name + '.csv'))
    num_records = 0
    for row in reader:
        record = OccurrenceRecord()
        record.latitude = float(row['Latitude - processed'])
        record.longitude = float(row['Longitude - processed'])
        record.species_scientific_name = row['Matched Scientific Name']
        yield record
        num_records += 1
    t = time.time() - t
    log.info('Read %d records in %0.2f seconds (%0.2f records/sec)',
             num_records, t, float(num_records) / t)

    zip_file.close()
    temp_zip_file.close()


def _facet_records_for_species(species_lsid, log):
    url = 'http://biocache.ala.org.au/ws/occurrences/facets/download'
    params = (
        ('q', 'lsid:' + species_lsid),
        ('fq', 'basis_of_record:HumanObservation'),
        ('fq', 'geospatial_kosher:true'),
        ('facets', 'lat_long'),
        ('count', 'true')
    )

    log.info('Requesting csv..')
    t = time.time()
    response = _fetch(url, params, True)
    log.info('Received response headers after %0.2f seconds', time.time() - t)

    reader = csv.reader(response)
    lat_long_heading, count_heading = reader.next()
    if lat_long_heading != 'lat_long':
        raise RuntimeError('Unexpected heading for lat_long facet')
    if count_heading != 'Count':
        raise RuntimeError('Unexpected heading for count')

    for row in reader:
        record = OccurrenceRecord()
        record.species_scientific_name = 'Dunno bro'
        record.latitude = float(row[0])
        record.longitude = float(row[1])
        count = int(row[2])
        for i in range(count):
            yield record


def _search_records_for_species(species_lsid, log):
    '''Can be slow. Could improve speed by fetching all pages in parallel
    instead of serially. Use the function records_for_species instead - see the
    documentation for details.'''

    url = 'http://biocache.ala.org.au/ws/occurrences/search'
    page_size = 1000
    current_page = 0
    params = [
        ('q', 'lsid:' + species_lsid),
        #maybe use a list of specific assertions instead of geospatial_kosher
        ('fq', 'geospatial_kosher:true'),
        #maybe also include basis_of_record:MachineObservation
        ('fq', 'basis_of_record:HumanObservation'),
        ('facet', 'off'),
        ('pageSize', page_size),
        #startIndex must be the last param (it is popped off the list later)
        ('startIndex', 0)
    ]

    while True:
        params.pop()
        params.append(('startIndex', current_page * page_size))

        t = time.time()
        response, response_size = _fetch_json(url, params, True)
        t = time.time() - t
        log.info('Received page %d, sized %0.2fkb in %0.2f secs (%0.2fkb/sec)',
                current_page + 1,
                float(response_size) / 1024.0,
                t,
                float(response_size) / 1024.0 / t)

        num_records = 0
        t = time.time()
        for occ in response['occurrences']:
            if _is_record_json_valid_for_modelling(occ):
                record = OccurrenceRecord()
                record.latitude = occ['decimalLatitude']
                record.longitude = occ['decimalLongitude']
                record.uuid = occ['uuid']
                record.species_lsid = occ['taxonConceptID']
                record.species_scientific_name = occ['scientificName']
                yield record
                num_records += 1
        t = time.time() - t
        log.info('Iterated over %d records in %0.2f seconds (%0.2f records/s)',
                 num_records, t, float(num_records) / t)

        total_pages = math.ceil(
                float(response['totalRecords']) / float(page_size))

        current_page += 1
        if current_page >= total_pages:
            break
