import textwrap
from pathlib import Path

import pytest

from miqa.arrayexpress import parse_sdrf, parse_sdrf_rows

FIXTURE_DIR = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Shared SDRF fixtures
# ---------------------------------------------------------------------------

# Minimal realistic SDRF with the most common columns
SDRF_BASIC = textwrap.dedent("""\
    Source Name\tCharacteristics[sex]\tCharacteristics[age]\tUnit[age]\tCharacteristics[organism part]\tFactor Value[disease]\tProtocol REF
    GSM001\tmale\t45\tyears\tblood\thealthy\tP-MTAB-1
    GSM002\tfemale\t32\tyears\tliver\tcarcinoma\tP-MTAB-1
""")

# Variant header style: spaces before brackets and Factor Value for organism part
SDRF_HEADER_VARIANTS = textwrap.dedent("""\
    Source Name\tCharacteristics [sex]\tCharacteristics [organism part]\tFactor Value [disease state]
    S1\tM\twhole blood\ttype 2 diabetes
""")

# Unit column present for age, extras column present
SDRF_EXTRAS_AND_UNITS = textwrap.dedent("""\
    Source Name\tCharacteristics[sex]\tCharacteristics[age]\tUnit[age]\tCharacteristics[passage number]\tCharacteristics[cell line]
    S1\tfemale\t60\tyears\t3\tMCF-7
""")

# Row where gender value is not in the known map
SDRF_UNKNOWN_GENDER = textwrap.dedent("""\
    Source Name\tCharacteristics[sex]
    S1\tintersex
""")

# Multiple rows including one with a missing (empty) value for a field
SDRF_PARTIAL_VALUES = textwrap.dedent("""\
    Source Name\tCharacteristics[organism part]\tCharacteristics[sex]
    S1\tbrain\tmale
    S2\t\tfemale
    S3\tcortex\t
""")

# First non-empty value wins when both Characteristics and Factor Value cover same field
SDRF_FIRST_VALUE_WINS = textwrap.dedent("""\
    Source Name\tCharacteristics[organism part]\tFactor Value[organism part]
    S1\tblood\tliver
""")


# ---------------------------------------------------------------------------
# parse_sdrf_rows — TSV parsing layer
# ---------------------------------------------------------------------------


class TestParseSdrfRows:
    def test_returns_one_dict_per_data_row(self):
        rows = parse_sdrf_rows(SDRF_BASIC)
        assert len(rows) == 2

    def test_keys_match_header_columns(self):
        rows = parse_sdrf_rows(SDRF_BASIC)
        assert 'Characteristics[sex]' in rows[0]
        assert 'Unit[age]' in rows[0]
        assert 'Protocol REF' in rows[0]

    def test_values_are_strings(self):
        rows = parse_sdrf_rows(SDRF_BASIC)
        assert rows[0]['Characteristics[sex]'] == 'male'
        assert rows[0]['Characteristics[age]'] == '45'

    def test_second_row_values(self):
        rows = parse_sdrf_rows(SDRF_BASIC)
        assert rows[1]['Source Name'] == 'GSM002'
        assert rows[1]['Characteristics[organism part]'] == 'liver'

    def test_empty_sdrf_returns_empty_list(self):
        assert parse_sdrf_rows('') == []

    def test_header_only_returns_empty_list(self):
        assert parse_sdrf_rows('Source Name\tCharacteristics[sex]\n') == []


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
def emtab14823_text():
    return (FIXTURE_DIR / 'E-MTAB-14823.sdrf.txt').read_text()


@pytest.fixture(scope='module')
def emtab14823_rows(emtab14823_text):
    return parse_sdrf_rows(emtab14823_text)


@pytest.fixture(scope='module')
def emtab14823(emtab14823_text):
    return parse_sdrf(emtab14823_text)


class TestEMTAB14823Rows:
    """Tests against the raw TSV parsing layer for E-MTAB-14823."""

    def test_row_count(self, emtab14823_rows):
        # 20 data rows (2 per sample × 10 samples)
        assert len(emtab14823_rows) == 20

    def test_source_name_present(self, emtab14823_rows):
        assert emtab14823_rows[0]['Source Name'] == 'DCA_1'

    def test_characteristics_age_raw(self, emtab14823_rows):
        assert emtab14823_rows[0]['Characteristics[age]'] == '16'

    def test_unit_column_is_time_unit_not_age(self, emtab14823_rows):
        # The real file uses Unit[time unit], not Unit[age].
        assert 'Unit[time unit]' in emtab14823_rows[0]
        assert emtab14823_rows[0]['Unit[time unit]'] == 'year'

    def test_array_data_file_present(self, emtab14823_rows):
        assert emtab14823_rows[0]['Array Data File'] == '205848680142_R08C01_Grn.idat'
        assert emtab14823_rows[1]['Array Data File'] == '205848680142_R08C01_Red.idat'


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
        assert extras.get('individual') == '1'
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
