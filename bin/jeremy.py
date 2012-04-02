#!/usr/bin/env python

import pathfix
import ala
import json
import urllib2


def scientific_names_from_prototype_site():
    host = 'http://spatialecology.jcu.edu.au'
    url = host + '/AustralianBirds/php/listspecies.php'

    j = json.load(urllib2.urlopen(url))
    for species in j['spp']:
        yield species['names'].rpartition('-')[2].strip()


def data_sources_for_records_for_species(scientific_name):
    lsid = ala.lsid_for_species_scientific_name(scientific_name)
    if lsid is None:
        return

    request = ala.create_request(ala.BIOCACHE + 'ws/occurrences/search', {
            'q': ala.q_param_for_lsid(lsid, kosher_only=False),
            'pageSize': 0,
            'facets': 'data_resource'})

    j = json.load(urllib2.urlopen(request))
    for source in j['facetResults'][0]['fieldResult']:
        yield source['label']


if __name__ == '__main__':
    already_printed = set()
    for scientific_name in scientific_names_from_prototype_site():
        for source in data_sources_for_records_for_species(scientific_name):
            if source not in already_printed:
                print source, '(contains ' + scientific_name + ')'
                already_printed.add(source)
