# DNA Microarray Database

A Python-based database system for managing and analyzing DNA microarray data.

## Overview

This project provides a PostgreSQL-backed database solution for storing,
querying, and managing DNA microarray experimental data. It includes schema
definitions and Python utilities for interacting with microarray datasets.

## Development

[uv](https://docs.astral.sh/uv/) is the simplest way to get setup with a Python
environment for this project. Download the uv binary as per the instructions on
their website, and get a environment with project dependencies using `uv sync`.
See more details on uv on their website.

### Running examples

Fetch metadata of 10 sample studies along with their idat files from GEO
```
uv run -m miqa.geo
```

Fetch metadata of a sample study from ArrayExpress (Biostudies)
```
uv run -m miqa.arrayexpress
```
