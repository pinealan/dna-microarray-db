from pprint import pprint

import miqa.geo as geo
from miqa.utils import streamed_download


if __name__ == "__main__":
    result = geo.list_studies()
    count = result.get('Count')
    id_list = result.get('IdList', {}).get('Id', [])

    print(f"Query total count: {count}")
    print(f"Number of IDs returned: {len(id_list)}")

    print(f"First ID {id_list[0]}")
    summary = geo.get_summary(id_list[0])
    study = geo.parse_summary_item(summary['DocSum']['Item'])
    files = geo.get_sample(study['Samples'][0]['Accession'])
    pprint(files)
    streamed_download(files[0].url, files[0].filename)
