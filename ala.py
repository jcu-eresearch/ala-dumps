import json
import urllib2
import urllib
import pprint
import os
import os.path
import sys

pp = pprint.PrettyPrinter(indent=4)

# Yellow Wagtail - Motacilla flava
# Total occurrence records: 23
WAGTAIL_LSID='urn:lsid:catalogueoflife.org:taxon:245fdbc0-60a7-102d-be47-00304854f810:ac2010'

# Tasmanian Tiger - Thylacinus cynocephalus
# Total occurrence records: 110
THYLACINE_LSID='urn:lsid:biodiversity.org.au:afd.taxon:80a0ed99-fcc0-4abf-8a14-760892eb2d86'

# Platypus - Ornithorhynchus anatinus
# Total occurrence records: 
PLATYPUS_LSID='urn:lsid:biodiversity.org.au:afd.taxon:33293f6d-0179-48f4-a12e-0c0e1ab0aa51'

# Grey Falcon - Falco (Hierofalco) hypoleucos
# Total occurrence records: 1501
FALCON_LSID='urn:lsid:biodiversity.org.au:afd.taxon:b1e0112f-3e9a-41d4-a205-1c0800f51306'

# Test record for adding and deleting assertions
FALCON_RECORD_ID='dr359|Falco hypoleucos|136446'


def make_request(url, params, use_get=True):
    paramString = urllib.urlencode(params)
    request = None
    if(use_get):
        url += '?{0}'.format(paramString)
        print >> sys.stderr, 'Requesting: {0}'.format(url);
        request = urllib2.Request(url)
    else:
        request = urllib2.Request(url, paramString)
    return request;


def occurence_json(lsid):
    url = 'http://biocache.ala.org.au/ws/occurrences/search'
    data = {
        'q': 'lsid:{0}'.format(lsid),
        'facet': 'off'
    };
    jsonStr = urllib2.urlopen(make_request(url, data)).read();
    j = json.loads(jsonStr)
    print json.dumps(j, sort_keys=True, indent=4)


def occurence_download(lsid):
    url = 'http://biocache.ala.org.au/ws/occurrences/download'
    data = {
        'q': 'wagtail',#.format(lsid),
        'email': 'tom.dalling@gmail.com',
        'reason': 'testing web services for AP03 project for JCU',
        'file': 'test'
    }
    req = make_request(url, data);
    response = urllib2.urlopen(req)

    pp.pprint(dict(response.info()))

    if os.path.exists('test.csv'):
        os.remove('test.csv')

    print "Writing chunks...",
    f = open('test.zip', 'w')
    while True:
        print '.',
        sys.stdout.flush()
        chunk = response.read(1024);
        if(len(chunk) > 0):
            f.write(chunk)
        else:
            break
    f.close()
    print ' done'

    os.system('unzip test.zip')
    if os.path.exists('citation.csv'):
        os.remove('citation.csv')
    os.remove('test.zip')


def add_assertion(recordId, assertion):
    """This fails with a 403 access forbidden error"""
    url = 'http://biocache.ala.org.au/ws/occurrences/assertions/add'
    data = {
        'recordUuid': recordId,
        'code': 0,
        'comment': assertion,
        'userId': 'tom.dalling+ala@gmail.com',
        'userDisplayName': 'Tom Dalling'
    }
    request = make_request(url, data, False)
    response = urllib2.urlopen(request)
    pp.pprint(dict(response.info()));

def get_assertion_list():
    url = 'http://biocache.ala.org.au/ws/assertions/codes'
    request = make_request(url, {})
    response = urllib2.urlopen(request)
    j = json.loads(response.read())
    print json.dumps(j, sort_keys=True, indent=4)


if __name__ == "__main__":
    try:
        get_assertion_list()
        #occurence_json(PLATYPUS_LSID)
    except urllib2.HTTPError as e:
        print "Failed with HTTP error code: {0}".format(e.code);
