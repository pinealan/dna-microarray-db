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
# IDF + SDRF parsing
# Spec: https://www.ebi.ac.uk/biostudies/misc/MAGE-TABv1.1_2011_07_28.pdf
# --------------------

# Protocol fields that form a parallel array (nth value of each row = nth protocol).
_PROTOCOL_FIELDS = [
    'Protocol Name',
    'Protocol Type',
    'Protocol Description',
    'Protocol Hardware',
    'Protocol Software',
    'Protocol Parameters',
    'Protocol Term Source REF',
    'Protocol Term Accession Number',
]

# Person fields that form a parallel array.
_PERSON_FIELDS = [
    'Person Last Name',
    'Person First Name',
    'Person Mid Initials',
    'Person Email',
    'Person Phone',
    'Person Fax',
    'Person Address',
    'Person Affiliation',
    'Person Roles',
]

_COMMENT_RE = re.compile(r'^Comment\[(.+)\]$', re.IGNORECASE)


def _idf_key(field: str, prefix: str) -> str:
    """Strip *prefix* from *field* and convert to snake_case dict key."""
    stem = field[len(prefix) :] if field.startswith(prefix) else field
    return stem.strip().lower().replace(' ', '_')


def _group_parallel(raw: dict[str, list[str]], fields: list[str], prefix: str) -> list[dict]:
    """Transpose parallel IDF array rows into a list of dicts.

    Uses the first field (e.g. Protocol Name / Person Last Name) as the
    canonical length; entries whose primary key is empty are skipped.
    """
    primary = raw.get(fields[0], [])
    n = len(primary)
    result = []
    for i in range(n):
        if not primary[i]:
            continue
        entry = {}
        for field in fields:
            arr = raw.get(field, [])
            entry[_idf_key(field, prefix)] = arr[i] if i < len(arr) else ''
        result.append(entry)
    return result


def parse_idf(text: str) -> dict:
    """Parse IDF (Investigation Description Format) tab-separated text.

    Each non-blank line is ``Tag\\tValue1\\tValue2\\t...``. Fields that
    represent parallel arrays (Protocol *, Person *) are transposed into
    lists of dicts.

    Returns a dict with keys:
        title, description, experimental_designs, experimental_factors,
        sdrf_files, date_of_experiment, public_release_date,
        pubmed_ids, publication_dois,
        protocols (list[dict]), persons (list[dict]),
        comments (dict[str, str]),
    """
    raw: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split('\t')
        tag = parts[0].strip()
        if not tag:
            continue
        values = [v.strip() for v in parts[1:]]
        # Strip trailing empty strings (common artefact of spreadsheet exports).
        while values and not values[-1]:
            values.pop()
        # Tags should not repeat in a valid IDF, but be defensive.
        if tag in raw:
            raw[tag].extend(values)
        else:
            raw[tag] = values

    def _scalar(tag: str) -> str | None:
        non_empty = [v for v in raw.get(tag, []) if v]
        return non_empty[0] if non_empty else None

    def _list(tag: str) -> list[str]:
        return [v for v in raw.get(tag, []) if v]

    comments: dict[str, str] = {}
    for tag, vals in raw.items():
        m = _COMMENT_RE.match(tag)
        if m:
            non_empty = [v for v in vals if v]
            if non_empty:
                comments[m.group(1)] = non_empty[0]

    return {
        'title': _scalar('Investigation Title'),
        'description': _scalar('Experiment Description'),
        'experimental_designs': _list('Experimental Design'),
        'experimental_factors': _list('Experimental Factor Name'),
        'sdrf_files': _list('SDRF File'),
        'date_of_experiment': _scalar('Date of Experiment'),
        'public_release_date': _scalar('Public Release Date'),
        'pubmed_ids': _list('PubMed ID'),
        'publication_dois': _list('Publication DOI'),
        'protocols': _group_parallel(raw, _PROTOCOL_FIELDS, 'Protocol '),
        'persons': _group_parallel(raw, _PERSON_FIELDS, 'Person '),
        'comments': comments,
        # 'raw': raw,
    }


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
    from pprint import pprint

    studies = list_studies()
    for study in tz.take(1, studies):
        accn = study['accession']
        pprint(get_study_metadata(accn))

        links = StudyLinks.from_accession(accn)
        pprint(parse_idf(httpx.get(links.idf).text))
        pprint(parse_sdrf(httpx.get(links.sdrf).text))


if __name__ == '__main__':
    from miqa.utils import setup_logging

    setup_logging()
    app()
