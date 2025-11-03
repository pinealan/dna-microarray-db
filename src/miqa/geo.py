"""
Gene Expression Omnibus (GEO) repository data retrieval utilities.

This module provides functions for querying the Gene Expression Omnibus (GEO) API,
collecting sample accession IDs, and downloading associated data files.

Summary on GEO concepts at https://www.ncbi.nlm.nih.gov/geo/info/overview.html

GPLxxx is a _platform_ type
GSMxxx is a sequencing _sample_, done on a particular platform
GSExxx is a _series_ of samples
GDSxxx is a _dataset_ of curated collection of comparable samples on GEO

Specific platforms
GPL13534: HumanMethylation450
GPL16304: HumanMethylation450 BeadChip
GPL21145: MethylationEPIC
"""


from dataclasses import dataclass
from typing import Any, cast

import httpx
import xmltodict
from bs4 import BeautifulSoup


E_UTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
GEO_ACCN_BASE = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
GEO_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo"


platforms = ['GPL13534', 'GPL21145', 'GPL16304']

# --------------------
# GEO Accession Display endpoint crawler/fetcher
# --------------------

def geo_lookup(accession_id, extra_params={}):
    url = GEO_ACCN_BASE
    params = {
        "acc": accession_id,
        "targ": "self",
        "view": "brief",
        "form": "text"
    } | extra_params
    res = httpx.get(url, params=params)
    return res.text
    # return parse_soft_lines(res.iter_lines())


def parse_soft_lines(lines):
    """Parse a sequence of lines that is in the Soft format"""
    parsed_entities = []
    current_entity = {}

    for line in lines:
        first_char = line[0]
        # New entity identified
        if first_char == '^':
            if len(current_entity) > 0:
                parsed_entities.append(current_entity)
                current_entity = {}
            entity_header = line[1:].strip().split(' = ')
            current_entity['entity_type'] = entity_header[0]
            current_entity['entity_id'] = entity_header[1]

        # Continuation of attributes for current entity
        elif first_char == '!':
            attr, val = line[1:].strip().split(' = ')
            attr_prefix_len = len(current_entity['entity_type']) + 1
            current_entity[attr[attr_prefix_len:]] = val

    # Wrap up last parsed entity
    if len(current_entity) > 0:
        parsed_entities.append(current_entity)

    return parsed_entities


# --------------------
# Entrez API crawler/fetcher
# --------------------

entrez_search_term = '(' + ' OR '.join([
    f'{platform}[accn]'
    for platform in platforms
]) + ') AND idat[suppFile]'

def list_studies():
    """TODO: Paginate through the results"""
    url = E_UTILS_BASE + "/esearch.fcgi"
    params = {
        "db": "gds",
        "term": entrez_search_term,
        # "retMax": 5000,
        "retMax": 5,
    }
    res = httpx.get(url, params=params)

    # Check if the request was successful
    if res.status_code != 200:
        raise Exception(f"Request failed with status code: {res.status_code}")

    # Parse the XML response into a dictionary
    data = xmltodict.parse(res.text)

    # Access specific elements (example)
    if 'eSearchResult' not in data:
        raise Exception(f"Data seems malformed. {data}")

    return data['eSearchResult']


def get_study_summary(id: int | str) -> dict:
    url = E_UTILS_BASE + "/esummary.fcgi"
    params = {
        "db": "gds",
        "id": id,
    }
    res = httpx.get(url, params=params)

    if res.status_code != 200:
        raise RuntimeError(
            f"Request for summary failed with status: {res.status_code}"
        )

    data = xmltodict.parse(res.text)
    return data['eSummaryResult']


def parse_field_value(field):
    ftype = field['@Type']

    if ftype == 'List':
        return [parse_field_value(node) for node in field.get('Item', [])]
    elif ftype == 'Structure':
        return {
            node['@Name']: parse_field_value(node)
            for node in field.get('Item', [])
        }
    elif ftype == 'String':
        return field.get('#text', '')
    elif ftype == 'Integer':
        if (x := field.get('#text')) is not None:
            return x
        else:
            return None


def parse_summary_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        field['@Name']: parse_field_value(field)
        for field in item
    }


# --------------------
# GEO files fetcher
# --------------------

@dataclass
class SampleSuppFile:
    accession_id: str
    filename: str
    url: str


def get_series(accession_id) -> list[SampleSuppFile]:
    url = GEO_FTP_BASE + f'/series/{accession_id[:-3]}nnn/{accession_id}/'
    res = httpx.get(url)

    if res.status_code != 200:
        raise RuntimeError(
            f"Request for sampling listing failed with status: {res.status_code}"
        )

    return res


def get_sample_files(accession_id) -> list[SampleSuppFile]:
    url = GEO_FTP_BASE + f'/samples/{accession_id[:-3]}nnn/{accession_id}/suppl/'
    res = httpx.get(url)

    if res.status_code != 200:
        raise RuntimeError(
            f"Request for sampling listing failed with status: {res.status_code}"
        )

    anchors = BeautifulSoup(res.text, 'html.parser').select('pre a')
    return [
        SampleSuppFile(accession_id, href, url + href)
        for a in anchors if (href := cast(str, a.get('href'))).endswith('.gz')
    ]


# Use module main as integration test
if __name__ == "__main__":
    from pprint import pprint

    query_res = list_studies()
    # pprint(query_res)

    id_list = query_res.get('IdList', {}).get('Id', [])
    study_id = id_list[0]

    summary = get_study_summary(study_id)
    study = parse_summary_item(summary['DocSum']['Item'])
    pprint(study)

    print('GEO lookup of a series')
    series = geo_lookup(study['Accession'], {"view": "full"})
    pprint(series)

    pprint(geo_lookup('GPL13534', {"targ": "self", "view": "brief"}))
