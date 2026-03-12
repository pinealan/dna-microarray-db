"""
ArrayExpress (by BioStudies) repository data retrieval utilities.
"""

import csv
import logging
import re
from dataclasses import dataclass
from io import StringIO
from typing import Any, Iterator, Self

import httpx
import toolz as tz
import typer

from miqa.error import MiqaError


BASE_FTP = 'https://ftp.ebi.ac.uk/biostudies/fire'
STUDY_BASE = 'https://www.ebi.ac.uk/biostudies/api/v1/studies'
SEARCH_BASE = 'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search'

logger = logging.getLogger(__spec__.name)


class AEError(MiqaError):
    """GEO related exceptions."""

    pass


def list_studies(page_size: int = 100) -> Iterator[dict]:
    """Return all studies matching methylation-by-array + idat filter, paginating as needed."""
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

    @classmethod
    def from_accession(cls, accession: str) -> Self:
        study = httpx.get(f'{STUDY_BASE}/{accession}/info').json()
        return cls(
            root=study['httpLink'],
            idf=study['httpLink'] + f'/Files/{accession}.idf.txt',
            sdrf=study['httpLink'] + f'/Files/{accession}.sdrf.txt',
            pagetab_json=study['httpLink'] + f'/{accession}.json',
            pagetab_tsv=study['httpLink'] + f'/{accession}.json',
        )


def _attrs_to_dict(attrs: list[dict]) -> dict:
    return {attr['name']: attr['value'] for attr in attrs if 'name' in attr and 'value' in attr}


def _parse_entity(node: dict) -> dict:
    if 'accno' in node:
        val = _attrs_to_dict(node.get('attributes', {}))
        val['node_type'] = node['type']
        return {node['accno']: val}
    else:
        return {
            node['type']: _attrs_to_dict(node.get('attributes', {})),
        }


def _walk_page_tab_json(node) -> Iterator[dict]:
    if isinstance(node, list):
        for n in node:
            yield from _walk_page_tab_json(n)
    elif isinstance(node, dict):
        yield _parse_entity(node)
        if 'subsections' in node:
            for n in node['subsections']:
                yield from _walk_page_tab_json(n)
    else:
        raise AEError('Unexpected node in page-tab json')


def get_study_metadata(accession: str) -> dict:
    """Parse a Page-TAB json into usable info."""
    resp = httpx.get(f'{STUDY_BASE}/{accession}').json()
    info = _attrs_to_dict(resp.get('attributes', []))

    # Walk the Page-tab JSON and extract all entities to top level, keyed by ID
    entities = tz.merge(*list(_walk_page_tab_json(resp['section'])))
    return info | {'entities': entities}


# --------------------
# SDRF metadata extraction
# Spec: https://www.ebi.ac.uk/biostudies/misc/MAGE-TABv1.1_2011_07_28.pdf
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

    Handles optional space before '[]': 'Characteristics [age]' and
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


def parse_sdrf(text: str) -> list[dict]:
    """Parse SDRF TSV text and extract structured metadata from every row."""
    return [extract_sdrf_metadata(row) for row in csv.DictReader(StringIO(text), delimiter='\t')]


# --------------------
# Study file downloader / crawler
# --------------------

app = typer.Typer(help='ArrayExpress crawler')


@app.command()
def import_one(accession: str = 'E-MTAB-14823'):

    from pprint import pprint

    # Get simple JSON info
    study_links = StudyLinks.from_accession(accession)
    pprint(list(csv.DictReader(StringIO(httpx.get(study_links.sdrf).text), delimiter='\t')))

    study_details = get_study_metadata(accession)
    pprint(study_details)

    # print(httpx.get(study_links.idf).text)
    # print(httpx.get(study_links.sdrf).text)
    # pprint(parse_sdrf(httpx.get(study_links.sdrf).text))


@app.command()
def crawl():
    studies = list_studies()


if __name__ == '__main__':
    from miqa.utils import setup_logging

    setup_logging()
    app()
