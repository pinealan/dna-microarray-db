"""
ArrayExpress (by BioStudies) repository data retrieval utilities.
"""

import csv
from io import StringIO

import httpx

from miqa.utils import streamed_download


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

BASE_FTP = "https://ftp.ebi.ac.uk/biostudies/fire"

def get_study_files(accession):
    prefix = '-'.join(accession.split('-')[:2]) + '-'
    suffix = accession[-3:]
    url_base = f"{BASE_FTP}/{prefix}/{suffix}/{accession}/Files/"
    res = httpx.get(url_base + f"{accession}.sdrf.txt")
    res = list(csv.DictReader(StringIO(res.text), delimiter='\t'))
    for f in res:
        streamed_download(url_base + f['Array Data File'], f['Array Data File'])
