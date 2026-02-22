from miqa.geo import *

def test_geo():
    res = geo_exact_lookup('GSE313496')
    assert res['platform_id'] == 'GPL21145'
    assert res['entity_id'] == 'GSE313496'
    assert res['sample_organism'] == 'Homo sapiens'
    assert len(res['sample_id']) > 10

    # TODO preprocess these from list[str] -> str
    assert len(res['overall_design']) > 0
    assert len(res['summary']) > 0
