"""
Summary of https://www.ncbi.nlm.nih.gov/geo/info/overview.html on GEO concepts

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
from pprint import pprint
from typing import Any, cast

import httpx
import xmltodict
from bs4 import BeautifulSoup


E_UTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
GEO_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo"


def geo_list_studies():
    url = E_UTILS_BASE + "/esearch.fcgi"
    params = {
        "db": "gds",
        "term": "(GPL13534[Platform] OR GPL21145[Platform] OR GPL16304[Platform]) AND idat[suppFile]",
        "retMax": 5000,
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


def geo_get_summary(id):
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


def parse_summary_item(item) -> dict[str, Any]:
    return {
        field['@Name']: parse_field_value(field)
        for field in item
    }


@dataclass
class SampleSuppFile:
    accession_id: str
    filename: str
    url: str


def geo_get_sample(accession_id) -> list[SampleSuppFile]:
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


def streamed_download(url, filename):
    with httpx.stream('GET', url) as response:
        with open(filename, 'wb') as f:
            for chunk in response.iter_bytes():
                f.write(chunk)


if __name__ == "__main__":
    try:
        result = geo_list_studies()
        count = result.get('Count')
        id_list = result.get('IdList', {}).get('Id', [])

        print(f"Query total count: {count}")
        print(f"Number of IDs returned: {len(id_list)}")

        print(f"First ID {id_list[0]}")
        summary = geo_get_summary(id_list[0])
        study = parse_summary_item(summary['DocSum']['Item'])
        files = geo_get_sample(study['Samples'][0]['Accession'])
        # pprint(files)
        print(files[0])
        streamed_download(files[0].url, files[0].filename)


    except Exception as e:
        raise
