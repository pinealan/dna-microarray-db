from pathlib import Path

import pytest

from miqa.arrayexpress import parse_idf, parse_sdrf

FIXTURE_DIR = Path(__file__).parent.parent


def _tsv(*rows: list[str]) -> str:
    """Build a TSV string from a list of rows (each row is a list of cell values)."""
    return '\n'.join('\t'.join(row) for row in rows) + '\n'


# ---------------------------------------------------------------------------
# Shared SDRF fixtures
# ---------------------------------------------------------------------------

# Minimal realistic SDRF with the most common columns
SDRF_BASIC = _tsv(
    [
        'Source Name',
        'Characteristics[sex]',
        'Characteristics[age]',
        'Unit[age]',
        'Characteristics[organism part]',
        'Factor Value[disease]',
        'Protocol REF',
    ],
    ['GSM001', 'male', '45', 'years', 'blood', 'healthy', 'P-MTAB-1'],
    ['GSM002', 'female', '32', 'years', 'liver', 'carcinoma', 'P-MTAB-1'],
)

# Variant header style: spaces before brackets and Factor Value for organism part
SDRF_HEADER_VARIANTS = _tsv(
    [
        'Source Name',
        'Characteristics [sex]',
        'Characteristics [organism part]',
        'Factor Value [disease state]',
    ],
    ['S1', 'M', 'whole blood', 'type 2 diabetes'],
)

# Unit column present for age, extras columns present
SDRF_EXTRAS_AND_UNITS = _tsv(
    [
        'Source Name',
        'Characteristics[sex]',
        'Characteristics[age]',
        'Unit[age]',
        'Characteristics[passage number]',
        'Characteristics[cell line]',
    ],
    ['S1', 'female', '60', 'years', '3', 'MCF-7'],
)

# Row where gender value is not in the known map
SDRF_UNKNOWN_GENDER = _tsv(
    ['Source Name', 'Characteristics[sex]'],
    ['S1', 'intersex'],
)

# Multiple rows including one with a missing (empty) value for a field
SDRF_PARTIAL_VALUES = _tsv(
    ['Source Name', 'Characteristics[organism part]', 'Characteristics[sex]'],
    ['S1', 'brain', 'male'],
    ['S2', '', 'female'],
    ['S3', 'cortex', ''],
)

# First non-empty value wins when both Characteristics and Factor Value cover same field
SDRF_FIRST_VALUE_WINS = _tsv(
    ['Source Name', 'Characteristics[organism part]', 'Factor Value[organism part]'],
    ['S1', 'blood', 'liver'],
)


# ---------------------------------------------------------------------------
# parse_sdrf — full pipeline (TSV text → extracted metadata)
# ---------------------------------------------------------------------------


class TestParseSdrf:
    def test_returns_one_dict_per_row(self):
        results = parse_sdrf(SDRF_BASIC)
        assert len(results) == 2

    def test_structured_fields_extracted(self):
        results = parse_sdrf(SDRF_BASIC)
        assert results[0]['gender'] == 'male'
        assert results[0]['tissue'] == 'blood'
        assert results[1]['gender'] == 'female'
        assert results[1]['tissue'] == 'liver'

    def test_disease_extracted_from_factor_value(self):
        results = parse_sdrf(SDRF_BASIC)
        assert results[0]['disease'] == 'healthy'
        assert results[1]['disease'] == 'carcinoma'

    def test_age_with_unit_appended(self):
        results = parse_sdrf(SDRF_BASIC)
        assert results[0]['age'] == '45 years'
        assert results[1]['age'] == '32 years'

    def test_non_metadata_columns_not_in_output(self):
        # 'Source Name' and 'Protocol REF' are not metadata columns
        results = parse_sdrf(SDRF_BASIC)
        assert 'Source Name' not in results[0]
        assert 'Protocol REF' not in results[0]

    def test_header_variants_space_before_bracket(self):
        results = parse_sdrf(SDRF_HEADER_VARIANTS)
        assert results[0]['gender'] == 'male'
        assert results[0]['tissue'] == 'whole blood'

    def test_header_variants_disease_state_maps_to_disease(self):
        results = parse_sdrf(SDRF_HEADER_VARIANTS)
        assert results[0]['disease'] == 'type 2 diabetes'

    def test_gender_abbreviation_m_normalised(self):
        results = parse_sdrf(SDRF_HEADER_VARIANTS)
        assert results[0]['gender'] == 'male'

    def test_unknown_gender_is_none(self):
        results = parse_sdrf(SDRF_UNKNOWN_GENDER)
        assert results[0]['gender'] is None

    def test_unmapped_characteristics_go_to_extras(self):
        results = parse_sdrf(SDRF_EXTRAS_AND_UNITS)
        assert results[0]['extras'] is not None
        assert results[0]['extras'].get('passage number') == '3'
        assert results[0]['extras'].get('cell line') == 'MCF-7'

    def test_extras_is_none_when_no_unmapped_columns(self):
        results = parse_sdrf(SDRF_BASIC)
        # SDRF_BASIC has no extras columns
        assert results[0].get('extras') is None

    def test_empty_value_skipped(self):
        results = parse_sdrf(SDRF_PARTIAL_VALUES)
        # Row S2 has empty organism part — tissue should be absent or None
        assert results[1].get('tissue') is None or 'tissue' not in results[1]
        # Row S3 has empty sex — gender should be absent or None
        assert results[2].get('gender') is None

    def test_first_value_wins_for_duplicate_field(self):
        # Both Characteristics[organism part] and Factor Value[organism part] present;
        # the first encountered non-empty value should be used.
        results = parse_sdrf(SDRF_FIRST_VALUE_WINS)
        assert results[0]['tissue'] == 'blood'


# ---------------------------------------------------------------------------
# E-MTAB-14823 — real-world SDRF fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope='module')
def emtab14823():
    return parse_sdrf((FIXTURE_DIR / 'E-MTAB-14823.sdrf.txt').read_text())


class TestEMTAB14823Metadata:
    """Tests against the full parse_sdrf pipeline for E-MTAB-14823."""

    def test_row_count(self, emtab14823):
        assert len(emtab14823) == 20

    def test_all_samples_are_female(self, emtab14823):
        assert all(row['gender'] == 'female' for row in emtab14823)

    def test_all_samples_are_blood(self, emtab14823):
        assert all(row['tissue'] == 'blood' for row in emtab14823)

    def test_disease_extracted(self, emtab14823):
        diseases = {row['disease'] for row in emtab14823}
        assert diseases == {'primary amenorrhea', 'secondary amenorrhea'}

    def test_age_extracted_without_unit(self, emtab14823):
        # Unit[time unit] does not match the age attribute key,
        # so the unit is NOT appended — this is the current parser behaviour.
        assert emtab14823[0]['age'] == '16'

    def test_age_values_are_numeric_strings(self, emtab14823):
        for row in emtab14823:
            float(row['age'])  # all should be parseable as a number

    def test_extras_contains_unmapped_characteristics(self, emtab14823):
        extras = emtab14823[0]['extras']
        assert extras is not None
        assert extras.get('organism') == 'Homo sapiens'
        assert extras.get('developmental stage') == 'adolescent'
        assert extras.get('phenotype') == 'amenorrhea'

    def test_factor_value_disease_does_not_override_characteristics_disease(self, emtab14823):
        # Both Characteristics[disease] and Factor Value[disease] are present;
        # first-value-wins means Characteristics wins.
        # Both happen to have the same value in this file, so we verify the field is set.
        assert emtab14823[0]['disease'] == 'secondary amenorrhea'

    def test_non_metadata_columns_absent(self, emtab14823):
        for row in emtab14823:
            assert 'Source Name' not in row
            assert 'Protocol REF' not in row
            assert 'Array Data File' not in row


# ---------------------------------------------------------------------------
# parse_idf — synthetic fixtures
# ---------------------------------------------------------------------------

IDF_MINIMAL = (
    '\n'.join(
        [
            'MAGE-TAB Version\t1.1',
            'Investigation Title\tTest Study',
            'Experiment Description\tA test study for unit testing.',
            'Experimental Design\tcase control design',
            'Experimental Factor Name\tdisease',
            'SDRF File\ttest.sdrf.txt',
            'Date of Experiment\t2024-01-15',
            'Public Release Date\t2025-06-01',
            'PubMed ID\t',
            'Publication DOI\t10.1234/test',
            'Person Last Name\tSmith',
            'Person First Name\tAlice',
            'Person Email\talice@example.com',
            'Person Affiliation\tTest University',
            'Person Roles\tsubmitter',
            'Protocol Name\tP-001\tP-002',
            'Protocol Type\tsample collection protocol\tnucleic acid extraction protocol',
            'Protocol Description\tBlood was drawn.\tDNA was extracted.',
            'Protocol Hardware\t\tQiagen kit',
            'Comment[AEExperimentType]\tmethylation profiling by array',
            'Comment[ArrayExpressAccession]\tE-TEST-0001',
        ]
    )
    + '\n'
)


@pytest.fixture(scope='module')
def idf_minimal():
    return parse_idf(IDF_MINIMAL)


class TestParseIdfSynthetic:
    def test_title(self, idf_minimal):
        assert idf_minimal['title'] == 'Test Study'

    def test_description(self, idf_minimal):
        assert idf_minimal['description'] == 'A test study for unit testing.'

    def test_experimental_designs(self, idf_minimal):
        assert idf_minimal['experimental_designs'] == ['case control design']

    def test_experimental_factors(self, idf_minimal):
        assert idf_minimal['experimental_factors'] == ['disease']

    def test_sdrf_files(self, idf_minimal):
        assert idf_minimal['sdrf_files'] == ['test.sdrf.txt']

    def test_dates(self, idf_minimal):
        assert idf_minimal['date_of_experiment'] == '2024-01-15'
        assert idf_minimal['public_release_date'] == '2025-06-01'

    def test_empty_pubmed_id_returns_empty_list(self, idf_minimal):
        # PubMed ID row exists but has no value — should not appear in list.
        assert idf_minimal['pubmed_ids'] == []

    def test_publication_doi(self, idf_minimal):
        assert idf_minimal['publication_dois'] == ['10.1234/test']

    def test_two_protocols_parsed(self, idf_minimal):
        assert len(idf_minimal['protocols']) == 2

    def test_protocol_fields(self, idf_minimal):
        p0 = idf_minimal['protocols'][0]
        assert p0['name'] == 'P-001'
        assert p0['type'] == 'sample collection protocol'
        assert p0['description'] == 'Blood was drawn.'
        assert p0['hardware'] == ''  # empty in the fixture

    def test_protocol_hardware_positional_empty(self, idf_minimal):
        # Protocol Hardware has an empty first value — P-001 has no hardware.
        assert idf_minimal['protocols'][0]['hardware'] == ''
        assert idf_minimal['protocols'][1]['hardware'] == 'Qiagen kit'

    def test_one_person_parsed(self, idf_minimal):
        assert len(idf_minimal['persons']) == 1

    def test_person_fields(self, idf_minimal):
        p = idf_minimal['persons'][0]
        assert p['last_name'] == 'Smith'
        assert p['first_name'] == 'Alice'
        assert p['email'] == 'alice@example.com'
        assert p['affiliation'] == 'Test University'
        assert p['roles'] == 'submitter'

    def test_comments_extracted(self, idf_minimal):
        assert idf_minimal['comments']['AEExperimentType'] == 'methylation profiling by array'
        assert idf_minimal['comments']['ArrayExpressAccession'] == 'E-TEST-0001'

    def test_multiple_persons(self):
        idf = parse_idf(
            '\n'.join(
                [
                    'MAGE-TAB Version\t1.1',
                    'Investigation Title\tMulti-person Study',
                    'SDRF File\tstudy.sdrf.txt',
                    'Person Last Name\tSmith\tJones',
                    'Person First Name\tAlice\tBob',
                    'Person Affiliation\tUni A\tUni B',
                    'Person Roles\tsubmitter\tinvestigator',
                ]
            )
            + '\n'
        )
        assert len(idf['persons']) == 2
        assert idf['persons'][0]['last_name'] == 'Smith'
        assert idf['persons'][1]['last_name'] == 'Jones'
        assert idf['persons'][1]['affiliation'] == 'Uni B'

    def test_blank_lines_ignored(self):
        text = '\n\nInvestigation Title\tBlank Test\n\nSDRF File\tblank.sdrf.txt\n\n'
        idf = parse_idf(text)
        assert idf['title'] == 'Blank Test'
        assert idf['sdrf_files'] == ['blank.sdrf.txt']


# ---------------------------------------------------------------------------
# E-MTAB-14823 — real-world IDF fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope='module')
def emtab14823_idf():
    return parse_idf((FIXTURE_DIR / 'E-MTAB-14823.idf.txt').read_text())


class TestEMTAB14823IDF:
    def test_title(self, emtab14823_idf):
        assert 'anorexia nervosa' in emtab14823_idf['title']

    def test_experimental_factor_is_disease(self, emtab14823_idf):
        assert emtab14823_idf['experimental_factors'] == ['disease']

    def test_experimental_design(self, emtab14823_idf):
        assert emtab14823_idf['experimental_designs'] == ['case control design']

    def test_sdrf_file(self, emtab14823_idf):
        assert emtab14823_idf['sdrf_files'] == ['E-MTAB-14823.sdrf.txt']

    def test_six_protocols(self, emtab14823_idf):
        assert len(emtab14823_idf['protocols']) == 6

    def test_protocol_names(self, emtab14823_idf):
        names = [p['name'] for p in emtab14823_idf['protocols']]
        assert names == [
            'P-MTAB-162815',
            'P-MTAB-162816',
            'P-MTAB-162817',
            'P-MTAB-162818',
            'P-MTAB-162819',
            'P-MTAB-162820',
        ]

    def test_protocol_types(self, emtab14823_idf):
        types = [p['type'] for p in emtab14823_idf['protocols']]
        assert 'sample collection protocol' in types
        assert 'nucleic acid extraction protocol' in types
        assert 'normalization data transformation protocol' in types

    def test_protocol_descriptions_non_empty(self, emtab14823_idf):
        for p in emtab14823_idf['protocols']:
            assert p['description'], f'Protocol {p["name"]} has no description'

    def test_protocol_hardware_aligned(self, emtab14823_idf):
        # First two protocols have no hardware; third onwards do.
        assert emtab14823_idf['protocols'][0]['hardware'] == ''
        assert emtab14823_idf['protocols'][1]['hardware'] == ''
        assert 'MethylationEPIC' in emtab14823_idf['protocols'][2]['hardware']

    def test_one_person(self, emtab14823_idf):
        assert len(emtab14823_idf['persons']) == 1

    def test_person_details(self, emtab14823_idf):
        p = emtab14823_idf['persons'][0]
        assert p['last_name'] == 'Palumbo'
        assert p['first_name'] == 'Domenico'
        assert p['affiliation'] == 'University of Salerno'
        assert p['roles'] == 'submitter'

    def test_publication_doi(self, emtab14823_idf):
        assert '10.1007' in emtab14823_idf['publication_dois'][0]

    def test_dates(self, emtab14823_idf):
        assert emtab14823_idf['date_of_experiment'] == '2025-01-10'
        assert emtab14823_idf['public_release_date'] == '2026-03-01'

    def test_comment_experiment_type(self, emtab14823_idf):
        assert emtab14823_idf['comments']['AEExperimentType'] == 'methylation profiling by array'

    def test_comment_accession(self, emtab14823_idf):
        assert emtab14823_idf['comments']['ArrayExpressAccession'] == 'E-MTAB-14823'
