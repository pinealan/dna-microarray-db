# CLAUDE.md

## Project overview

DNA methylation microarray sample database. Crawls GEO and ArrayExpress to
discover samples, enrich and normalise sample metadata attributes using rules,
downloads corresponding IDATs to S3 compitable storage, extracts QC metrics,
persists metadata and metrics to PostgreSQL.

### GEO concepts

- GPLxxx = platform, GSMxxx = sample, GSExxx = series
- Supported platforms: GPL13534, GPL16304 (HumanMethylation450), GPL21145 (MethylationEPIC)
- SOFT format parsed by `SoftParser` in `geo.py`

### ArrayExpress concepts

- See API docs at https://www.ebi.ac.uk/biostudies/help#
- Raw submission metadata are in MAGE-TAB (which has IDF and SDRF sub-formats)

### Metadata approach

Heterogeneous / best-effort. Structured fields (gender, age, tissue, disease,
extraction protocol, hybridization protocol) extracted where available;
everything else goes in `sample.extras` jsonb column as archive.

## Development

### Package manager

Always use `uv`. Run code with `uv run`, install deps with `uv add`.

### Key modules

- `src/miqa/geo.py` — GEO crawler (NCBI Gene Expression Omnibus)
- `src/miqa/arrayexpress.py` — ArrayExpress/BioStudies crawler
- `src/miqa/db.py` — psycopg3 DB helpers (raw SQL, no ORM)
- `src/miqa/storage.py` — S3/DigitalOcean Spaces upload/delete
- `src/miqa/config.py` — environment variable config
- `src/miqa/utils.py` — streamed_download, setup_logging

### Database

PostgreSQL. Schema in `schema.psql`. Env var: `DATABASE_URL`.
Tables: `repository`, `platform`, `sample`, `idat_file`.

### Storage

S3-compatible (DigitalOcean Spaces). IDATs are transient: uploaded for
processing, deleted after QC metrics are extracted. Track lifecycle via
`idat_file.uploaded_at / processed_at / deleted_at`.

## Running

```bash
uv run -m miqa geo              # crawl GEO
uv run -m miqa arrayexpress     # crawl ArrayExpress
uv run pytest test/             # run tests
uv run ruff check src/ test/    # lint
uv run ruff format src/ test/   # format
```

## After making code changes

Always run tests and lint after any code change, and fix any errors that come up:

```bash
uv run pytest test/
uv run ruff check src/ test/
```

- Fix all pytest failures before considering the task done.
- Fix all ruff errors. For ruff warnings that are not errors (exit code 0), use judgement — fix them if they indicate a real issue, skip if they are noisy false positives.
- Do not suppress errors with `# noqa` unless there is a genuine reason (e.g. a dynamic SQL column name that cannot be parameterised); leave a comment explaining why.
