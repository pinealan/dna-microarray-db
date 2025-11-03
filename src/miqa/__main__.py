import miqa.geo as geo
import miqa.arrayexpress as ae
from miqa.utils import streamed_download


def fetch_geo():
    """GEO query crawler for studies and samples to retrieve idat files."""
    query_res = geo.list_studies()
    id_list = query_res.get('IdList', {}).get('Id', [])

    for study_id in id_list:
        summary = geo.get_study_summary(study_id)
        study = geo.parse_summary_item(summary['DocSum']['Item'])
        for sample in study['Samples']:
            # TODO: Collect sample metadata
            sample_accession = sample['Accession']
            files = geo.get_sample(sample_accession)
            for f in files:
                streamed_download(f.url, f.filename)


def fetch_arrayexpress():
    query_res = ae.list_studies()
    # for study in query_res:
    #     study['accession']

    study = query_res[0]
    return ae.get_study_files(study['accession'])


if __name__ == "__main__":
    # fetch_geo()
    fetch_arrayexpress()
