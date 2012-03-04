import sys
import pprint
import ala

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

pp = pprint.PrettyPrinter(indent=4)


for record in ala.records_for_species(FALCON_LSID):
    pp.pprint(record)


#def occurence_download(lsid):
    #url = 'http://biocache.ala.org.au/ws/occurrences/download'
    #data = {
        #'q': 'wagtail',#.format(lsid),
        #'email': 'tom.dalling@gmail.com',
        #'reason': 'testing web services for AP03 project for JCU',
        #'file': 'test'
    #}
    #req = make_request(url, data);
    #response = urllib2.urlopen(req)
#
    #pp.pprint(dict(response.info()))
#
    #if os.path.exists('data/test.csv'):
        #os.remove('data/test.csv')
#
    #print "Writing chunks...",
    #f = open('test.zip', 'w')
    #while True:
        #print '.',
        #sys.stdout.flush()
        #chunk = response.read(1024);
        #if(len(chunk) > 0):
            #f.write(chunk)
        #else:
            #break
    #f.close()
    #print ' done'
#
    #os.system('unzip test.zip')
    #if os.path.exists('citation.csv'):
        #os.remove('citation.csv')
    #if os.path.exists('test.csv'):
        #shutil.move('test.csv', 'data/test.csv')
    #os.remove('test.zip')
