"""
ArrayExpress (by BioStudies) repository data retrieval utilities.
"""

import csv
import logging
import re
import tempfile
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Iterator

import httpx
import toolz as tz

from miqa.utils import guess_idat_channel, streamed_download


logger = logging.getLogger()


BASE_FTP = 'https://ftp.ebi.ac.uk/biostudies/fire'
STUDY_BASE = 'https://www.ebi.ac.uk/biostudies/api/v1/studies'
SEARCH_BASE = 'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search'


def list_studies(page_size: int = 100) -> Iterator[dict]:
    """Return all studies matching methylation-by-array + idat filter, paginating as needed."""
    hits = []
    page = 1
    while True:
        res = httpx.get(
            SEARCH_BASE,
            params={
                'facet.study_type': 'methylation profiling by array',
                'facet.file_type': 'idat',
                'pageSize': page_size,
                'page': page,
            },
        )
        data = res.json()
        page_hits = data.get('hits', [])
        yield from page_hits
        if len(page_hits) == 0 or data.get('totalHits', 0) <= data.get('pageSize', 0) * data.get(
            'page', 0
        ):
            break
        page += 1


@dataclass
class StudyLinks:
    root: str
    idf: str
    sdrf: str
    pagetab_json: str
    pagetab_tsv: str

    def datafile(self, filename: str) -> str:
        """
        Return a URL to a data file assuming the study follows the standard layout with
        a 'Files/' subdirectory.
        """
        return self.root + '/Files/' + filename


def get_study_links(accession: str) -> StudyLinks:
    study = httpx.get(f'{STUDY_BASE}/{accession}/info').json()
    return StudyLinks(
        root=study['httpLink'],
        idf=study['httpLink'] + f'/Files/{accession}.idf.txt',
        sdrf=study['httpLink'] + f'/Files/{accession}.sdrf.txt',
        pagetab_json=study['httpLink'] + f'/{accession}.json',
        pagetab_tsv=study['httpLink'] + f'/{accession}.json',
    )


def study_details(accession: str) -> dict:
    resp = httpx.get(f'{STUDY_BASE}/{accession}').json()

    info = {
        'accession': accession,
        'metadata': lift_attrs(tz.dissoc(resp, 'section')),
        'ftp_dir': httpx.get(f'{STUDY_BASE}/{accession}/info').json().get('ftpLink'),
    }
    section = resp.get('section', {})
    if section:
        info |= attrs_to_dict(section.get('attributes', []))
        info['child_items'] = {
            s.get('accno'): lift_attrs(s)
            for s in section.get('subsections', [])
            if isinstance(s, dict)
        }
    return info


def lift_attrs(item: dict) -> dict:
    return tz.dissoc(item, 'attributes') | attrs_to_dict(item.get('attributes', []))


def attrs_to_dict(attrs: list[dict]) -> dict:
    return {attr['name']: attr['value'] for attr in attrs if 'name' in attr and 'value' in attr}


# --------------------
# SDRF metadata extraction
# --------------------

# Maps SDRF bracketed attribute names to structured DB fields.
# Both Characteristics[] and Factor Value[] are matched against this.
_ATTR_FIELD_MAP = {
    'organism part': 'tissue',
    'tissue': 'tissue',
    'cell type': 'tissue',
    'disease': 'disease',
    'disease state': 'disease',
    'sex': 'gender',
    'gender': 'gender',
    'age': 'age',
}

_GENDER_MAP = {
    'male': 'male',
    'm': 'male',
    'female': 'female',
    'f': 'female',
}

# Column prefixes that carry biological metadata values
_VALUE_PREFIXES = frozenset(['characteristics', 'factor value'])


def _parse_sdrf_col(col: str) -> tuple[str, str | None]:
    """Return (prefix, attribute) for an SDRF column name.

    Handles optional space before '[': 'Characteristics [age]' and
    'Characteristics[age]' are both parsed as ('characteristics', 'age').
    """
    m = re.match(r'^(.+?)\s*\[(.+)\]$', col.strip())
    if m:
        return m.group(1).strip().lower(), m.group(2).strip().lower()
    return col.strip().lower(), None


def extract_sdrf_metadata(row: dict) -> dict[str, Any]:
    """
    Extract structured metadata from an SDRF row dict.

    Handles Characteristics[], Factor Value[], and Unit[] columns per the
    MAGE-TAB v1.1 spec.  Unit values are appended to their corresponding
    attribute values (important for age: "45" + "years" → "45 years").
    Both Characteristics and Factor Value columns are treated equally; the
    first non-empty value for each field wins.
    """
    structured: dict[str, Any] = {}
    extras: dict[str, Any] = {}

    # Collect units first (Unit[attr] columns follow their value columns)
    units: dict[str, str] = {}
    for col, val in row.items():
        prefix, attr = _parse_sdrf_col(col)
        if prefix == 'unit' and attr and val and val.strip():
            units[attr] = val.strip()

    for col, val in row.items():
        prefix, attr = _parse_sdrf_col(col)
        if prefix not in _VALUE_PREFIXES or not attr or not val or not val.strip():
            continue

        val = val.strip()
        if attr in units:
            val = f'{val} {units[attr]}'

        field = _ATTR_FIELD_MAP.get(attr)
        if field and field not in structured:
            structured[field] = val
        elif field is None:
            extras.setdefault(attr, val)

    # Normalise gender
    gender_raw = structured.pop('gender', None)
    if gender_raw:
        structured['gender'] = _GENDER_MAP.get(gender_raw.lower())

    structured['extras'] = extras or None
    return structured


def parse_sdrf_rows(text: str) -> list[dict]:
    """Parse raw SDRF TSV text into a list of row dicts (column name → raw value)."""
    return list(csv.DictReader(StringIO(text), delimiter='\t'))


def parse_sdrf(text: str) -> list[dict]:
    """Parse SDRF TSV text and extract structured metadata from every row."""
    return [extract_sdrf_metadata(row) for row in parse_sdrf_rows(text)]


# --------------------
# Study file downloader / crawler
# --------------------


if __name__ == '__main__':
    import json
    from ftplib import FTP
    from pprint import pprint
    from urllib.parse import urlparse

    from miqa.utils import setup_logging

    setup_logging()

    studies = list_studies()
    # study_brief = next(studies)
    # pprint(study_brief)
    # accession = study_brief['accession']
    # accession = 'E-MTAB-14823'
    # study = study_details(accession)
    # pprint(study)

    ## Get raw PageTab JSON
    # print(json.dumps(httpx.get(f'{STUDY_BASE}/{accession}').json()))

    # Inspect out PageTab JSON and see if it is worth parsing
    # study = json.load(open('e-mtab-14823.json'))
    # pprint(study)
    # pprint(study.keys())

    # Get simple JSON info
    for study_brief in tz.take(5, studies):
        accession = study_brief['accession']
        study_links = get_study_links(accession)
        print(httpx.get(study_links.idf).text)
        print(httpx.get(study_links.sdrf).text)
        # study = httpx.get(f'{STUDY_BASE}/{accession}/info').json()
        # uri = urlparse(study['httpLink'])
        # res = httpx.get(study['httpLink'] + '/Files/')
        # pprint(res.text)
