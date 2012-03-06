import time
import math
import urllib
import urllib2
import json
import pdb


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


@_retry()
def _fetch_json(url, params=False, use_get=False):
    """Opens the url and returns the result of urllib2.urlopen"""

    if params:
        params = urllib.urlencode(params)
        if use_get:
            url += '?' + params
            params = False

    if params:
        response = urllib2.urlopen(url, params)
    else:
        response = urllib2.urlopen(url)

    return_value = json.load(response)
    if len(return_value) == 0:
        throw RuntimeError('ALA returned empty response')
    else:
        return return_value


def _is_record_json_valid_for_modelling(record):
    #TODO: get list of assertions from Jeremy to check here
    return True


def records_for_species(species_lsid):
    """A generator for OccurrenceRecord objects fetched from ALA

    Could improve speed by fetching all pages in parallel.
    """

    # uses 'occurrences/search' instead of 'occurrences/download' because of the
    # download limit, which can be bypassed by getting the records one page at
    # a time
    url = 'http://biocache.ala.org.au/ws/occurrences/search'
    page_size = 400
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
        response = _fetch_json(url, params, True)

        for occ in response['occurrences']:
            if _is_record_json_valid_for_modelling(occ):
                record = OccurrenceRecord()
                record.latitude = occ['decimalLatitude']
                record.longitude = occ['decimalLongitude']
                record.uuid = occ['uuid']
                record.species_lsid = occ['taxonConceptID']
                record.species_scientific_name = occ['scientificName']
                yield record

        total_pages = math.ceil(
                float(response['totalRecords']) / float(page_size))

        current_page += 1
        if current_page >= total_pages:
            break


class OccurrenceRecord(object):
    """Plain old data structure for an occurrence record"""

    def __init__(self):
        self.latitude = None
        self.longitude = None
        self.uuid = None
        self.species_lsid = None
        self.species_scientific_name = None

    def __repr__(self):
        return '<record species="{species}" uuid="{uuid}" latLong="{lat}, {lng}" />'.format(
                species=self.species_scientific_name,
                uuid=self.uuid,
                lat=self.latitude,
                lng=self.longitude)
