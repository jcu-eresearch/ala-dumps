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
def _fetchJson(url, params=False, use_get=False):
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

    return json.read(response)


def records_for_species(species_lsid):
    """A generator for OccurrenceRecord objects fetched from ALA"""

    pageSize = 1000
    currentPage = 0
    url = 'http://biocache.ala.org.au/ws/occurrences/search'
    params = [
        ('q', 'lsid:' + species_lsid),
        #maybe use a list of specific assertions instead of geospatial_kosher
        ('fq', 'geospatial_kosher:true'),
        #maybe also include basis_of_record:MachineObservation
        ('fq', 'basis_of_record:HumanObservation'),
        ('facet', 'off'),
        ('pageSize', pageSize),
        #startIndex must be the last param (it is popped off the list later)
        ('startIndex', 0)
    ]

    while True:
        params.pop()
        params.append(('startIndex', currentPage * pageSize))
        response = _fetchJson(url, params, True)

        for occ in response['occurrences']:
            record = OccurrenceRecord()
            record.latitude = occ['decimalLatitude']
            record.longitude = occ['decimalLongitude']
            record.uuid = occ['uuid']
            yield record

        totalPages = math.ceil(
                float(response['totalRecords']) / float(pageSize))

        currentPage += 1
        if currentPage >= totalPages:
            break


class OccurrenceRecord(object):
    """Plain old data structure for an occurrence record"""

    def __init__(self):
        self.latitude = 0.0
        self.longitude = 0.0
        self.uuid = None

    def __repr__(self):
        return '<record uuid="{uuid}" latLong="{lat}, {lng}" />'.format(
                uuid=self.uuid,
                lat=self.latitude,
                lng=self.longitude)
