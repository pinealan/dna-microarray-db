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

import asyncio
import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Iterable

import httpx
import psycopg
import typer
import toolz as tz

from miqa import db, storage
from miqa.utils import assert_list_str, guess_idat_channel, streamed_download
from miqa.error import MiqaError


E_UTILS_BASE = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils'
GEO_ACCN_BASE = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi'
GEO_FTP_BASE = 'https://ftp.ncbi.nlm.nih.gov/geo'

logger = logging.getLogger(__spec__.name)


class GEOError(MiqaError):
    """GEO related exceptions."""

    pass


class GEODataError(MiqaError):
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
    params = {'acc': accession_id, 'targ': 'self', 'view': 'brief', 'form': 'text'} | extra_params
    res = httpx.get(url, params=params)
    return parse_soft_lines(res.iter_lines())


def geo_exact_lookup(accession_id: str, *args, **kwargs) -> dict:
    res = geo_lookup(accession_id, *args, **kwargs)
    if len(res) > 1:
        raise ValueError(f'Received more than 1 item from exact lookup of {accession_id}')
    return res[0]


async def _geo_lookup_async(
    client: httpx.AsyncClient,
    accession_id: str,
    extra_params: dict = {},
) -> list[dict]:
    params = {'acc': accession_id, 'targ': 'self', 'view': 'brief', 'form': 'text'} | extra_params
    res = await client.get(GEO_ACCN_BASE, params=params)
    return parse_soft_lines(res.text.splitlines())


async def fetch_samples_async(
    sample_ids: list[str],
    concurrency: int = 5,
) -> list[tuple[str, dict | Exception]]:
    """Fetch multiple GEO sample records in parallel.

    Returns a list of (sample_id, result) pairs where result is either a parsed
    sample dict or an Exception if the request failed.
    """
    sem = asyncio.Semaphore(concurrency)

    async def fetch_one(client: httpx.AsyncClient, sample_id: str):
        async with sem:
            records = await _geo_lookup_async(client, sample_id)
            if len(records) != 1:
                raise ValueError(f'Expected 1 record for {sample_id}, got {len(records)}')
            return records[0]

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[fetch_one(client, sid) for sid in sample_ids],
            return_exceptions=True,
        )

    return list(zip(sample_ids, results))


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
            attr, _, val = line[1:].strip().partition(' = ')
            if val.strip() == '':
                return

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
    url = E_UTILS_BASE + '/esearch.fcgi'
    params = {
        'db': 'gds',
        'retMax': 10000,
        'retmode': 'json',
    } | extra_params
    res = httpx.get(url, params=params)

    # Check if the request was successful
    if res.status_code != 200:
        raise GEOError(f'Request failed with status code: {res.status_code}')

    data = res.json()

    # Access specific elements (example)
    if 'esearchresult' not in data:
        raise GEODataError(f'Data seems malformed. {data}')

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
    url = E_UTILS_BASE + '/esummary.fcgi'
    params = {
        'db': 'gds',
        'id': id,
        'retmode': 'json',
    }
    res = httpx.get(url, params=params)

    if res.status_code != 200:
        raise RuntimeError(f'Request for summary failed with status: {res.status_code}')

    return res.json()


platforms = ['GPL13534', 'GPL21145', 'GPL16304']

series_with_idat_search_term = ' AND '.join(
    ['idat[suppFile]', 'gse[Entry Type]', f'({" OR ".join([p + "[accn]" for p in platforms])})']
)


def geo_series_id_iter() -> Iterable[str]:
    """Return an iterator of GEO series IDs."""
    entrez_ids = e_search_all(term=series_with_idat_search_term)
    # TODO: eSummary supports fetching more than 1 ID at a time. We can save lots of
    # network calls if we do a batched query.
    for eids in tz.partition_all(100, entrez_ids):
        res = e_summary(','.join(eids))['result']
        for eid in eids:
            # Look into entrez's record for corresponding GEO accession ID
            yield res[eid]['accession']


# --------------------
# Metadata extraction
# --------------------


def find_idat_files(sample: dict) -> list[str] | None:
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


def join_series_sample_attrs(sample: dict, series: dict) -> dict:
    return sample | {
        'series_summary': series['summary'],
        'series_design': series['overall_design'],
    }


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


def upsert_sample(sample, series, conn):
    with conn.cursor() as cur:
        cur.execute(
            """

            INSERT INTO sample (
                repository_id, repository_sample_id, repository_series_id,
                platform_id, source_metadata, normalised_metadata
            ) VALUES (
                'geo', %s, %s, %s, %s, %s
            )
            ON CONFLICT (repository_id, repository_sample_id) DO UPDATE
            SET source_metadata = EXCLUDED.source_metadata,
                normalised_metadata = EXCLUDED.normalised_metadata
            RETURNING id;
            """,
            (
                sample['entity_id'],
                series['entity_id'],
                sample['platform_id'],
                json.dumps(join_series_sample_attrs(sample, series)),
                None,  # TODO: Do we normalise metadata right away?
            ),
        )
        row = cur.fetchone()
        if row is not None:
            return row[0]

        raise db.DBError('Could not upsert row')


# --------------------
# GEO crawler entrypoint
# --------------------


def crawl_and_process(
    conn: psycopg.Connection,
    dry_run: bool = False,
):
    if not dry_run and conn is None:
        raise RuntimeError('DB connection must be provided when not dry-run.')

    # TODO: Lots of parallelisation possible in this workflow
    cnt = 0

    # TODO: eSummary supports fetching more than 1 ID at a time. We can save lots of
    # network calls if we do a batched query.
    for series_id in geo_series_id_iter():
        series = geo_exact_lookup(series_id)

        for sample_id in series['sample_id']:
            # TODO: Re-retrieve sample after a certain period of time has passed since
            # it was last inspected/processed so we get a change to see updates
            if db.seen_sample(conn, 'geo', sample_id):
                logger.debug('Seen sample')
                continue

            # Process a single sample
            sample = geo_exact_lookup(sample_id)
            if (idat_files := find_idat_files(sample)) is None:
                continue

            # Dryrun short circuit and log
            if dry_run:
                cnt += 1
                logger.info(f'(dry-run) {cnt=} Would insert sample {sample_id} {sample=}')
                for fpath in idat_files:
                    logger.info(f'(dry-run) Would upload {fpath}')
                continue
            assert conn is not None

            # Save sample to database
            try:
                if (
                    db_id := save_sample_and_idat(sample_id, sample, idat_files, conn)
                ) is not None:
                    cnt += 1
                    logger.info(f'{cnt=} {sample_id} inserted as {db_id=}')
                else:
                    logger.warning(f'Could not insert {sample_id} into DB')
            except Exception as e:
                logger.error(e, f'Failed to save {sample_id}')


def save_sample_and_idat(
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
            channel=guess_idat_channel(filename),
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


app = typer.Typer(help='GEO crawler')


@app.command()
def import_one(series_id: str):
    import miqa.config as cfg

    conn = psycopg.connect(cfg.DATABASE_URL, autocommit=True)
    series = geo_exact_lookup(series_id)

    # TODO: Extract metadata from series, so we can pass it on later to samples
    # series_enriched = enrich_series(series)

    for sample_id in series['sample_id']:
        # TODO: Re-retrieve sample after a certain period of time has passed since
        # it was last inspected/processed so we get a change to see updates
        if db.seen_sample(conn, 'geo', sample_id):
            logger.debug('Seen sample')
            continue

        # Process a single sample
        sample = geo_exact_lookup(sample_id)
        if (idat_files := find_idat_files(sample)) is None:
            continue
        db_id = upsert_sample(sample, series, conn)
        logger.info(f'{sample_id} inserted as {db_id=}')


@app.command()
def load_and_dump():
    import json
    from pprint import pprint

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
    # series_accn = res_esum['result'][eid]['accession']
    series_accn = 'GSE318173'
    res_series = geo_exact_lookup(series_accn)
    json.dump(res_series, open(series_accn + '.json', 'w'))

    # Get GEO record of Sample
    sample_accn = res_series['sample_id'][0]
    res_sample = geo_exact_lookup(sample_accn)
    json.dump(res_sample, open(sample_accn + '.json', 'w'))


@app.command()
def show_enrich(series_accn: str = 'GSE318173'):
    import json

    res_series = json.load(open(series_accn + '.json'))
    sample_accn = res_series['sample_id'][0]
    res_sample = json.load(open(sample_accn + '.json'))

    # TODO: Use rules from miqa.normalise
    # print(json.dumps(enrich_sample(res_sample)))


@app.command()
def show_raw(
    series_accn: str = 'GSE318173',
):
    from pprint import pprint

    res_series = geo_exact_lookup(series_accn)
    sample_accn = res_series['sample_id'][0]
    res_sample = geo_exact_lookup(sample_accn)
    pprint(res_series)
    pprint(res_sample)


@app.command()
def crawl(
    skip_seen: bool = True,
    concurrency: int = 10,
):
    import miqa.config as config

    conn = psycopg.connect(config.DATABASE_URL, autocommit=True)
    cnt = 1
    for series_id in geo_series_id_iter():
        try:
            series = geo_exact_lookup(series_id)
        except Exception:
            logger.exception(f'Failed to lookup {series_id=}')
            continue

        unseen_ids = [
            sid
            for sid in series['sample_id']
            if not (skip_seen and db.seen_sample(conn, 'geo', sid))
        ]
        logger.debug(f'{series_id=}: {len(series["sample_id"])} samples, {len(unseen_ids)} unseen')

        if not unseen_ids:
            continue

        # Fetch all unseen samples for this series in parallel, then write sequentially.
        results = asyncio.run(fetch_samples_async(unseen_ids, concurrency=concurrency))

        for sample_id, sample_or_exc in results:
            if isinstance(sample_or_exc, Exception):
                logger.error(f'Failed to fetch {sample_id}: {sample_or_exc}')
                continue
            try:
                db_id = upsert_sample(sample_or_exc, series, conn)
                logger.info(f'{cnt=} {sample_id} inserted as {db_id=}')
                cnt += 1
            except psycopg.errors.ForeignKeyViolation:
                logger.error(f'Failed to insert sample {sample_id=} with uncatalogued attribute')
            except Exception:
                logger.exception(f'Failed to insert {sample_id=}')


# Use module main as integration test or quick script
if __name__ == '__main__':
    from miqa.utils import setup_logging

    setup_logging()
    app()
