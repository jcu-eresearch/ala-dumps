import time
import re
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
import logging


#occurrence records per request for 'search' strategy
PAGE_SIZE = 1000


log = logging.getLogger(__name__)


class OccurrenceRecord(object):
    '''Plain old data structure for an occurrence record'''

    def __init__(self):
        self.latitude = None
        self.longitude = None
        self.uuid = None

    def __repr__(self):
        return '<record uuid="{uuid} latLong="{lat}, {lng}" />'.format(
            uuid=self.uuid,
            lat=self.latitude,
            lng=self.longitude)


def records_for_species(species_lsid, strategy):
    '''A generator for OccurrenceRecord objects fetched from ALA'''

    if strategy == 'search':
        return _search_records_for_species(species_lsid)
    elif strategy == 'download':
        return _downloadzip_records_for_species(species_lsid)
    elif strategy == 'facet':
        return _facet_records_for_species(species_lsid)
    else:
        raise ValueError('Invalid strategy: ' + strategy)


def species_scientific_name_for_lsid(species_lsid):
    '''Fetches the scientific name of a species from its LSID'''

    guid = urllib.quote(species_lsid)
    url = 'http://bie.ala.org.au/species/shortProfile/{0}.json'.format(guid)
    info, size = _fetch_json(url, check_not_empty=False)
    if not info or len(info) == 0:
        return None
    else:
        return info['scientificName']


def lsid_for_species_scientific_name(scientific_name):
    '''Fetches the LSID of a species from its scientific name.

    If you were to take the lsid this function returns, and get the species
    name from the lsid, it may not be the same species name. This is because
    species names can change, and ALA will map old incorrect names to new
    correct names.'''

    url = 'http://bie.ala.org.au/ws/guid/' + urllib.quote(scientific_name)
    info, size = _fetch_json(url, check_not_empty=False)
    if not info or len(info) == 0:
        return None
    else:
        return info[0]['identifier']


def _retry(tries=3, delay=2, backoff=2):
    '''A decorator that retries a function or method until it succeeds (success
    is when the function completes and no exception is raised).

    delay sets the initial delay in seconds, and backoff sets the factor by
    which the delay should lengthen after each failure. backoff must be greater
    than 1, or else it isn't really a backoff. tries must be at least 1, and
    delay greater than 0.'''

    if backoff <= 1:
        raise ValueError('backoff must be greater than 1')

    tries = math.floor(tries)
    if tries < 1:
        raise ValueError('tries must be >= 1')

    if delay <= 0:
        raise ValueError('delay must be >= 0')

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


def _request(url, params=None, use_get=True):
    '''URL encodes params and fetches the url via GET or POST'''
    if params:
        params = urllib.urlencode(params)
        if use_get:
            url += '?' + params
            params = None

    if params:
        return urllib2.urlopen(url, params)
    else:
        return urllib2.urlopen(url)


@_retry()
def _fetch_json(url, params=None, check_not_empty=True, use_get=True):
    '''Fetches and parses the JSON at the given url.

    Returns the object parsed from the JSON, and the size (in bytes) of the
    JSON text that was fetched'''

    response = _request(url, params, use_get)
    response_str = response.read()
    return_value = json.loads(response_str)
    if check_not_empty and len(return_value) == 0:
        raise RuntimeError('ALA returned empty response')
    else:
        return return_value, len(response_str)


@_retry()
def _fetch(url, params=None, use_get=True):
    '''Opens the url and returns the result of urllib2.urlopen'''
    return _request(url, params, use_get)


def _query_for_lsid(species_lsid):
    '''The 'q' parameter for ALA web service queries

    Maybe use a list of specific assertions instead of geospatial_kosher.'''

    return _strip_n_squeeze('''

        lsid:{lsid} AND
        geospatial_kosher:true AND
        (
            basis_of_record:HumanObservation OR
            basis_of_record:MachineObservation
        )

        '''.format(lsid=species_lsid))


def _strip_n_squeeze(q):
    r'''Strips and squeezes whitespace. Completely G rated, I'll have you know.

    >>> _strip_n_squeeze('    hello   \n   there my \r\n  \t  friend    \n')
    'hello there my friend'
    '''

    return re.sub(r'[\s]+', r' ', q.strip())


def _chunked_read_and_write(infile, outfile):
    '''Reads from infile and writes to outfile in chunks, while logging speed
    info'''

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


def _downloadzip_records_for_species(species_lsid):
    '''This strategy is too slow. The requested file size is small, but ALA
    can't generate the file fast enough so the download speed won't go above
    8kb/s'''

    file_name = 'data'
    url = 'http://biocache.ala.org.au/ws/occurrences/download'
    params = {
        'q': _query_for_lsid(species_lsid),
        'fields': 'decimalLatitude.p,decimalLongitude.p,scientificName.p',
        'email': 'tom.dalling@gmail.au',
        'reason': 'AP03 project for James Cook University',
        'file': file_name
    }

    #need to write zip file to a temp file
    log.info('Requesting zip file from ALA...')
    t = time.time()
    response = _fetch(url, params)
    log.info('Response headers received after %0.2f seconds', time.time() - t)

    log.info('Downloading zip file...')
    log.debug('Response headers: %s', dict(response.info()))
    temp_zip_file = tempfile.TemporaryFile()
    t = time.time()
    _chunked_read_and_write(response, temp_zip_file)
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
        yield record
        num_records += 1
    t = time.time() - t
    log.info('Read %d records in %0.2f seconds (%0.2f records/sec)',
             num_records, t, float(num_records) / t)

    zip_file.close()
    temp_zip_file.close()


def _facet_records_for_species(species_lsid):
    '''Fastest strategy, but each record only contains latitude and longitude.

    Using the '/occurrences/faces/download' web service, there is no way to get
    other info about the record, like assertions and the record uuid. If
    bandwidth wasn't an issue, the 'search' strategy may be just as fast as
    this one.'''

    url = 'http://biocache.ala.org.au/ws/occurrences/facets/download'
    params = {
        'q': _query_for_lsid(species_lsid),
        'facets': 'lat_long',
        'count': 'true'
    }

    log.info('Requesting csv..')
    t = time.time()
    response = _fetch(url, params)
    log.info('Received response headers after %0.2f seconds', time.time() - t)

    reader = csv.reader(response)
    lat_long_heading, count_heading = reader.next()
    if lat_long_heading != 'lat_long':
        raise RuntimeError('Unexpected heading for lat_long facet')
    if count_heading != 'Count':
        raise RuntimeError('Unexpected heading for count')

    num_records = 0
    for row in reader:
        record = OccurrenceRecord()
        record.latitude = float(row[0])
        record.longitude = float(row[1])
        count = int(row[2])
        for i in range(count):
            yield record
            num_records += 1
            if num_records % 1000 == 0:
                log.info('%d records done...', num_records)


def _search_records_for_species(species_lsid):
    '''Currently the best strategy.

    Faster than 'download' strategy. More info about each record than 'facet'
    strategy.

    Speed could maybe be improved by fetching every page concurrently, instead
    of serially.'''

    url = 'http://biocache.ala.org.au/ws/occurrences/search'
    params = {
        'q': _query_for_lsid(species_lsid),
        'fl': 'id,latitude,longitude',
        'facet': 'off',
        'pageSize': PAGE_SIZE,
    }

    current_page = 0
    while True:
        params['startIndex'] = current_page * PAGE_SIZE

        t = time.time()
        response, response_size = _fetch_json(url, params)
        t = time.time() - t
        log.info('Received page %d, sized %0.2fkb in %0.2f secs (%0.2fkb/s)',
                current_page + 1,
                float(response_size) / 1024.0,
                t,
                float(response_size) / 1024.0 / t)

        for occ in response['occurrences']:
            record = OccurrenceRecord()
            record.latitude = occ['decimalLatitude']
            record.longitude = occ['decimalLongitude']
            record.uuid = occ['uuid']
            yield record

        total_pages = math.ceil(
                float(response['totalRecords']) / float(PAGE_SIZE))

        current_page += 1
        if current_page >= total_pages:
            break


if __name__ == "__main__":
    import doctest
    doctest.testmod()
