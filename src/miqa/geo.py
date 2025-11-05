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

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import httpx
from bs4 import BeautifulSoup

from miqa.utils import streamed_download


E_UTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
GEO_ACCN_BASE = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
GEO_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo"

platforms = ['GPL13534', 'GPL21145', 'GPL16304']

logger = logging.getLogger()


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
    return parse_soft_lines(res.iter_lines())


def geo_exact_lookup(accession_id, *args, **kwargs):
    res = geo_lookup(accession_id, *args, **kwargs)
    if len(res) > 1:
        raise ValueError(f'Received more than 1 item from exact lookup of {accession_id}')
    return res[0]


class SoftParser:
    """
    Parser for the SOFT format used by GEO data repo.

    Does not support parsing data tables.
    """
    def __init__(self):
        self.parsed = []
        self.current = {}

    def parse_line(self, line):
        first_char = line[0]
        # New entity identified
        if first_char == '^':
            if len(self.current) > 0:
                self.parsed.append(self.current)
                self.current = {}
            entity_header = line[1:].strip().split(' = ')
            self.current['entity_type'] = entity_header[0]
            self.current['entity_id'] = entity_header[1]

        # Continuation of attributes for current entity
        elif first_char == '!':
            split_res = line[1:].strip().split(' = ')
            if len(split_res) < 2:
                return

            attr, val = split_res
            attr_prefix_len = len(self.current['entity_type']) + 1
            attr = attr[attr_prefix_len:]
            if attr in self.current:
                if isinstance(self.current[attr], list):
                    self.current[attr].append(val)
                else:
                    self.current[attr] = [self.current[attr], val]
            else:
                self.current[attr] = val

    def parse_lines(self, lines):
        for line in lines:
            try:
                self.parse_line(line)
            except Exception as e:
                print(line, file=sys.stderr)
                raise e
        if len(self.current) > 0:
            self.parsed.append(self.current)

        return self.parsed


def parse_soft_lines(lines):
    """Parse a sequence of lines that is in the Soft format"""
    return SoftParser().parse_lines(lines)


# --------------------
# Entrez API crawler/fetcher
# --------------------

entrez_search_term = '(' + ' OR '.join([
    f'{platform}[accn]'
    for platform in platforms
]) + ') AND idat[suppFile]'


def e_search_series(extra_params={}):
    """TODO: Paginate through the results"""
    url = E_UTILS_BASE + "/esearch.fcgi"
    params = {
        "db": "gds",
        "term": entrez_search_term,
        "retMax": 10000,
        "retmode": "json",
    } | extra_params
    res = httpx.get(url, params=params)

    # Check if the request was successful
    if res.status_code != 200:
        raise Exception(f"Request failed with status code: {res.status_code}")

    data = res.json()

    # Access specific elements (example)
    if 'esearchresult' not in data:
        raise Exception(f"Data seems malformed. {data}")

    return data['esearchresult']


def e_summary(id: int | str) -> dict:
    url = E_UTILS_BASE + "/esummary.fcgi"
    params = {
        "db": "gds",
        "id": id,
        "retmode": "json",
    }
    res = httpx.get(url, params=params)

    if res.status_code != 200:
        raise RuntimeError(
            f"Request for summary failed with status: {res.status_code}"
        )

    return res.json()


# --------------------
# GEO files fetcher
# --------------------

@dataclass
class SampleSuppFile:
    accession_id: str
    filename: str
    url: str


def geo_ftp_series(accession_id) -> httpx.Response:
    url = GEO_FTP_BASE + f'/series/{accession_id[:-3]}nnn/{accession_id}/'
    res = httpx.get(url)

    if res.status_code != 200:
        raise RuntimeError(
            f"Request for sampling listing failed with status: {res.status_code}"
        )

    return res


def geo_ftp_ls_sample_files(accession_id) -> list[SampleSuppFile]:
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


# --------------------
# GEO crawler entrypoint
# --------------------
# TODO
# [ ] Walk the list of series/samples to aggregate metadata
# [ ] Download files and put them to S3
# [ ] Insert sample details (S3 paths and metadata) into DB


def collect_idats_of_platform(platform):
    geo_info = geo_exact_lookup(platform)

    all_series = geo_info['series_id']
    all_samples = geo_info['sample_id']
    logger.debug(f'Series count = {len(all_series)}')
    logger.debug(f'Sample count = {len(all_samples)}')

    # tmp for dev
    max_fetch = 10
    cnt_fetch = 0

    for sample_id in all_samples[100000:]:

        # tmp for dev
        if cnt_fetch >= max_fetch:
            return

        sample = geo_exact_lookup(sample_id)

        if sample['supplementary_file'] == 'NONE':
            logger.debug(f'Sample has no supplementary files {sample_id=}')
            continue

        for fpath in sample['supplementary_file']:
            fpath = 'https' + fpath[3:]
            # TODO: Stream to S3
            streamed_download(fpath, Path(fpath).name)

        # TODO: Construct and insert sample detail

        cnt_fetch += 1
        pprint(sample)
        logger.info(f'Sample idats fetched {sample_id=}')


def collect_idats():
    for plat in platforms:
        collect_idats_of_platform(plat)


# Use module main as integration test
if __name__ == "__main__":
    from pprint import pprint
    from miqa.utils import setup_logging

    setup_logging()
    logger.setLevel(logging.DEBUG)

    collect_idats_of_platform(platforms[0])
