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
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast, Iterable

import httpx
import psycopg
from bs4 import BeautifulSoup

from miqa import db, storage
from miqa.utils import assert_list_str, streamed_download


E_UTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
GEO_ACCN_BASE = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
GEO_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo"

logger = logging.getLogger(__name__)


class GEOError(Exception):
    """GEO related exceptions."""
    pass


class GEODataError(GEOError):
    """Found invalid GEO data during parsing."""
    pass


# --------------------
# GEO Accession Display endpoint fetcher
# --------------------

def geo_lookup(accession_id: str, extra_params={}) -> list[dict]:
    """
    Core API access of GEO records.

    See 'Construct a URL' section on https://www.ncbi.nlm.nih.gov/geo/info/download.html
    for details of the query parameters.
    """
    url = GEO_ACCN_BASE
    params = {
        "acc": accession_id,
        "targ": "self",
        "view": "brief",
        "form": "text"
    } | extra_params
    res = httpx.get(url, params=params)
    return parse_soft_lines(res.iter_lines())


def geo_exact_lookup(accession_id: str, *args, **kwargs) -> dict:
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


def parse_soft_lines(lines: Iterable[str]) -> list[dict]:
    """Parse a sequence of lines that is in the Soft format"""
    return SoftParser().parse_lines(lines)


# --------------------
# Entrez API crawler
# --------------------

def e_search(**extra_params):
    """
    Query the Entrez eSearch program.

    See the following links for docs on the endpoint.
    - https://www.ncbi.nlm.nih.gov/geo/info/qqtutorial.html
    - https://www.ncbi.nlm.nih.gov/geo/info/geo_paccess.html
    """
    url = E_UTILS_BASE + "/esearch.fcgi"
    params = {
        "db": "gds",
        "retMax": 10000,
        "retmode": "json",
    } | extra_params
    res = httpx.get(url, params=params)

    # Check if the request was successful
    if res.status_code != 200:
        raise GEOError(f"Request failed with status code: {res.status_code}")

    data = res.json()

    # Access specific elements (example)
    if 'esearchresult' not in data:
        raise GEODataError(f"Data seems malformed. {data}")

    return data['esearchresult']


def e_search_all(**extra_params):
    """
    Paginate through results of a query on the Entrez eSearch program.
    """
    res = e_search(**extra_params)
    while (n := len(res['idlist'])) > 0:
        yield from res['idlist']
        res = e_search(**extra_params, retstart=int(res['retstart']) + n)


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
# GEO FTP fetcher
# (unused as of now)
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
# Metadata extraction
# --------------------


def enrich_series(series: dict):
    """TODO"""
    return


def find_idat_files(sample) -> list[str] | None:
    """Extract the idat file FTP paths of the given sample."""
    series_id = sample['series_id']

    # Ensure there are supplementary files
    supp = sample.get('supplementary_file', 'NONE')
    if supp == 'NONE' or len(supp) == 0:
        logger.debug(f'Sample has no supplementary files {series_id=}')
        return

    # Ensure there are idat(s)
    assert_list_str(supp)
    idat_files = [f for f in supp if '.idat' in f.lower()]
    if not idat_files:
        logger.debug(f'No IDAT files for {series_id=}')
        return

    return idat_files


_CHAR_PATTERNS: list[tuple[str, str]] = [
    # Each tuple is (Regex pattern, Attribute name)
    (r'tissue\s*:', 'tissue'),
    (r'tissue type\s*:', 'tissue'),
    (r'source tissue\s*:', 'tissue'),
    (r'cell type\s*:', 'tissue'),
    (r'disease\s*:', 'disease'),
    (r'disease state\s*:', 'disease'),
    (r'diagnosis\s*:', 'disease'),
    (r'gender\s*:', 'gender'),
    (r'sex\s*:', 'gender'),
    (r'age\s*:', 'age'),
    (r'age at diagnosis\s*:', 'age'),
]

_GENDER_MAP = {
    'male': 'male',
    'm': 'male',
    'female': 'female',
    'f': 'female',
}


def parse_characteristic(line: str) -> tuple[str, str] | None:
    """
    Parse a single characteristics_ch1 string like 'tissue: blood'.
    Returns (field_name, value) for the first recognised field, or None if unrecognised.
    """
    for pattern, field in _CHAR_PATTERNS:
        m = re.match(pattern, line, re.IGNORECASE)
        if m:
            extracted = line[m.end():].strip()
            return field, extracted
    return None


def enrich_sample(sample: dict) -> dict[str, Any]:
    """
    Pull structured metadata out of a GEO SOFT sample dict.
    Returns a dict ready to be passed to db.upsert_sample().
    """
    structured: dict[str, Any] = {}
    extras: dict[str, Any] = {}

    # Series (parent GSE, if there are multiple of them, treat the first as canonical)
    series_id = sample.get('series_id')
    if isinstance(series_id, list):
        series_id = series_id[0]
    structured['series_id'] = series_id

    # Platform
    structured['platform_id'] = sample.get('platform_id')

    # Parse characteristics_ch1 entries
    chars = sample.get('characteristics_ch1', [])
    if isinstance(chars, str):
        chars = [chars]

    remaining_chars = []
    for char in chars:
        parsed = parse_characteristic(char)
        if parsed:
            field, val = parsed
            if field not in structured:
                structured[field] = val
        else:
            remaining_chars.append(char)

    if remaining_chars:
        extras['characteristics_ch1'] = remaining_chars

    # Extraction protocol
    structured['extraction_protocol'] = sample.get('extract_protocol_ch1')

    # Normalise gender
    gender_raw = structured.pop('gender', None)
    if gender_raw:
        structured['gender'] = _GENDER_MAP.get(gender_raw.lower().strip())


    # Everything else goes to extras
    skip_keys = {
        'entity_type', 'entity_id', 'series_id', 'platform_id',
        'characteristics_ch1', 'extract_protocol_ch1', 'supplementary_file',
    }
    for k, v in sample.items():
        if k not in skip_keys and k not in structured:
            extras[k] = v

    # TODO: fields to be processed
    # [x] characteristics_ch1
    # [ ] data_processing
    # [ ] extract_protocol_ch1
    # [ ] growth_protocol_ch1
    # [ ] hyb_protocol
    # [ ] label_ch1
    # [ ] label_protocol_ch1
    # [ ] hyb_protocol
    # [ ] scan_protocol
    # [ ] source_name_ch1

    structured['extras'] = extras or None
    return structured


# --------------------
# GEO crawler entrypoint
# --------------------


platforms = ['GPL13534', 'GPL21145', 'GPL16304']


series_with_idat_search_term = ' AND '.join([
    'idat[suppFile]',
    'gse[Entry Type]',
    f'({' OR '.join([p + '[accn]' for p in platforms])})'
])


def crawl_and_process(
    conn: psycopg.Connection | None = None,
    dry_run: bool = False,
):
    if not dry_run and conn is None:
        raise RuntimeError('DB connection must be provided when not dry-run.')

    entrez_ids = e_search_all(term=series_with_idat_search_term)
    cnt = 0

    # TODO: Check against database to avoid retreading processed series/samples

    # TODO: eSummary supports fetching more than 1 ID at a time. We can save lots of
    # network calls if we do a batched query.
    for eid in entrez_ids:

        # Look into entrez's record for corresponding GEO accession ID
        series_id = e_summary(eid)['result'][eid]['accession']
        series = geo_exact_lookup(series_id)

        # TODO: Extract metadata from series, so we can pass it on later to samples
        #series_enriched = enrich_series(series)

        for sample_id in series['sample_id']:

            # Process a single sample
            sample = geo_exact_lookup(sample_id)
            if (idat_files := find_idat_files(sample)) is None:
                continue
            sample_enriched = enrich_sample(sample)

            # Dryrun short circuit and log
            if dry_run:
                cnt += 1
                logger.info(f'(dry-run) {cnt=} Would insert sample {sample_id} {sample_enriched=}')
                for fpath in idat_files:
                    logger.info(f'(dry-run) Would upload {fpath}')
                continue
            assert conn is not None

            # Save sample to database
            if (
                db_id := save_sample(sample_id, sample_enriched, idat_files, conn)
            ) is not None:
                cnt += 1
                logger.info(f'{cnt=} {sample_id=} inserted as {db_id=}')


def save_sample(
    sample_id: str,
    sample_enriched: dict,
    idat_files: list[str],
    conn: psycopg.Connection,
) -> int | None:

    # Insert sample as DB record
    db_sample_id = db.upsert_sample(
        conn,
        repository_id='geo',
        repository_sample_id=sample_id,
        **sample_enriched,
    )

    # Insert each idat as DB record and download to storage
    for fpath in idat_files:

        # Replace ftp:// with https://
        # We are using HTTP to fetch the files instead of FTP because their FTP
        # server doesn't seem to work
        fpath = 'https' + fpath[3:] if fpath.startswith('ftp') else fpath
        filename = Path(fpath).name
        s3_key = f'geo/{sample_id}/{filename}'

        idat_id = db.insert_idat_file(
            conn,
            sample_id=db_sample_id,
            source_url=fpath,
            channel=get_idat_channel(filename),
        )

        # Download file to tmp local storage, then upload to S3
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            streamed_download(fpath, str(local_path))
            storage.upload_file(local_path, s3_key)

            # TODO perhaps we should also process the idat file(s) rightaway, and S3
            # serves more as a short term mirror such that we can have easy access
            # to the file if inspections are needed? (like 30 retention)

        db.mark_idat_uploaded(conn, idat_id, s3_key)
        logger.info(f'Uploaded {s3_key}')

    return db_sample_id


def get_idat_channel(filename: str) -> str:
    if '_Grn' in filename:
        return 'Grn'
    elif '_Red' in filename:
        return 'Red'
    else:
        raise GEODataError()


# Use module main as integration test
if __name__ == "__main__":
    from pprint import pprint
    from miqa.utils import setup_logging

    setup_logging()

    # Search Entrez records
    res_esearch = e_search(
        term=series_with_idat_search_term,
        retMax=10,
    )
    pprint(res_esearch)

    # Get Entrez record
    eid = res_esearch['idlist'][0]
    res_esum = e_summary(eid)
    pprint(res_esum)

    # Get GEO record of Series
    series_accn = res_esum['result'][eid]['accession']
    res_geo = geo_exact_lookup(series_accn)
    pprint(res_geo)

    # Get GEO record of Sample
    sample_accn = res_geo['sample_id'][0]
    res_geo = geo_exact_lookup(sample_accn)
    pprint(res_geo)
