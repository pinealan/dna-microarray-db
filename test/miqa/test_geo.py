import pytest

from miqa.geo import geo_exact_lookup, enrich_sample


@pytest.fixture(scope='session')
def series():
    return geo_exact_lookup('GSE313496')


@pytest.fixture(scope='session')
def sample():
    return geo_exact_lookup('GSM9368910')


def test_series(series):
    assert series['platform_id'] == 'GPL21145'
    assert series['entity_id'] == 'GSE313496'
    assert series['sample_organism'] == 'Homo sapiens'
    assert len(series['sample_id']) > 10

    # TODO preprocess these from list[str] -> str
    assert len(series['overall_design']) > 0
    assert len(series['summary']) > 0


def test_sample(sample):
    res = enrich_sample(sample)
    assert res['tissue'] == 'whole blood'
    assert res['gender'] == 'male'
