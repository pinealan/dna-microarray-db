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

from pprint import pprint

import httpx
import xmltodict


E_UTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


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


def geo_get_study(id):
    url = E_UTILS_BASE + "/esummary.fcgi"
    params = {
        "db": "gds",
        "id": id,
    }
    res = httpx.get(url, params=params)

    if res.status_code != 200:
        raise Exception(f"Request failed with status code: {res.status_code}")

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


def parse_summary_item(item):
    return {
        field['@Name']: parse_field_value(field)
        for field in item
    }


if __name__ == "__main__":
    try:
        result = geo_list_studies()
        count = result.get('Count')
        id_list = result.get('IdList', {}).get('Id', [])

        print(f"Query total count: {count}")
        print(f"Number of IDs returned: {len(id_list)}")

        print(f"First ID {id_list[0]}")
        pprint(parse_summary_item(geo_get_study(id_list[0])['DocSum']['Item']))

    except Exception as e:
        raise
