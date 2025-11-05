"""
ArrayExpress (by BioStudies) repository data retrieval utilities.
"""

import csv
import logging
from io import StringIO

import httpx
import toolz as tz

from miqa.utils import streamed_download


logger = logging.getLogger()


BASE_FTP = "https://ftp.ebi.ac.uk/biostudies/fire"
STUDY_BASE = "https://www.ebi.ac.uk/biostudies/api/v1/studies"

def list_studies():
    res = httpx.get(
        "https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search",
        params={
            "facet.study_type": "methylation profiling by array",
            "facet.file_type": "idat",
            "pageSize": 100
        }
    )
    return res.json()['hits']


def study_details(accession):
    resp = httpx.get(f"{STUDY_BASE}/{accession}").json()

    # Reorganise study struct into something that makes a bit more sense
    info = {
        'metadata': lift_attrs(tz.dissoc(resp, 'section')),
        'ftp_dir': httpx.get(f"{STUDY_BASE}/{accession}/info").json()['ftpLink']
    }
    info |= attrs_to_dict(resp['section']['attributes'])
    info['child_items'] = {
        s.get('accno'): lift_attrs(s)
        for s in resp['section']['subsections']
        if isinstance(s, dict)
    }
    return info


def lift_attrs(item):
    return tz.dissoc(item, 'attributes') | attrs_to_dict(item['attributes'])


def attrs_to_dict(attrs):
    return {
        attr['name']: attr['value']
        for attr in attrs
    }


def get_study_files(accession):
    prefix = '-'.join(accession.split('-')[:2]) + '-'
    suffix = accession[-3:]
    url_base = f"{BASE_FTP}/{prefix}/{suffix}/{accession}/Files/"
    res = httpx.get(url_base + f"{accession}.sdrf.txt")
    res = list(csv.DictReader(StringIO(res.text), delimiter='\t'))
    for f in res:
        streamed_download(url_base + f['Array Data File'], f['Array Data File'])


if __name__ == "__main__":
    from pprint import pprint
    from miqa.utils import setup_logging

    setup_logging()
    logger.setLevel(logging.DEBUG)

    studies = list_studies()
    study_brief = studies[-1]
    study = study_details(study_brief ['accession'])
    pprint(study)
