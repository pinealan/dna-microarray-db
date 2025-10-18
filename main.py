import httpx
import xmltodict

def main():
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
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

    result = data['eSearchResult']
    count = result.get('Count')
    id_list = result.get('IdList', {}).get('Id', [])

    print(f"Query total count: {count}")
    print(f"Number of IDs returned: {len(id_list)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Program errored {e}")
