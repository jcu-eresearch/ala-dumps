import sys
import csv
import string
import ala


def show_help():
    print """
    Writes an occurrence record CSV to standard output

    Usage:
        python {scriptname} <LSID>

    Below are some example LSID values, but they are not persistant so they may
    have changed by now.

        Yellow Wagtail - Motacilla flava
        urn:lsid:catalogueoflife.org:taxon:d1d4d6f7-2dc5-11e0-98c6-2ce70255a436:col20110201

        Grey Falcon - Falco (Hierofalco) hypoleucos
        urn:lsid:biodiversity.org.au:afd.taxon:b1e0112f-3e9a-41d4-a205-1c0800f51306

        Powerful Owl - Ninox (Rhabdoglaux) strenua
        urn:lsid:biodiversity.org.au:afd.taxon:a396b19c-14b1-4b53-bd08-6536a53abec9

        Australian Magpie - Cracticus tibicen
        WARNING: 358,037 occurrence records
        urn:lsid:biodiversity.org.au:afd.taxon:b76f8dcf-fabd-4e48-939c-fd3cafc1887a
    """.format(scriptname=sys.argv[0])


def spp_code_for_species_name(species_name):
    """Uppercase alpha-only name with length <= 8

    Not sure if this is too restrictive, but Jeremy's example uses
    "GOULFINC" for "Gould Finch"
    """
    allowed_chars = frozenset(string.ascii_letters + ' ')
    filtered = ''.join([c for c in species_name if c in allowed_chars]);
    filtered = filtered.upper();
    parts = filtered.split(' ')
    if len(parts) > 1:
        return parts[0].strip()[:4] + parts[-1].strip()[:4]
    else:
        return filtered.strip()[:8]


def write_csv_for_species_lsid(species_lsid):
    writer = csv.writer(sys.stdout)
    writer.writerow(['SPPCODE', 'LATDEC', 'LONGDEC'])
    sppCode = None
    for record in ala.records_for_species(species_lsid):
        if sppCode is None:
            sppCode = spp_code_for_species_name(record.species_scientific_name)
        writer.writerow([sppCode, record.latitude, record.longitude])


if __name__ == '__main__':
    if len(sys.argv) > 1:
        write_csv_for_species_lsid(sys.argv[1])
    else:
        show_help()

