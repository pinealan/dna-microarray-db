import pytest

from miqa.normalise import (
    STRUCTURED_FIELDS,
    apply_rules_to_sample,
    first_matching_rule,
    match_value,
)


# ---------------------------------------------------------------------------
# match_value
# ---------------------------------------------------------------------------


class TestMatchValueSubstring:
    def test_exact_match(self):
        assert match_value('whole blood', 'whole blood', 'substring')

    def test_partial_match(self):
        assert match_value('peripheral whole blood cells', 'whole blood', 'substring')

    def test_case_insensitive(self):
        assert match_value('Whole Blood', 'whole blood', 'substring')
        assert match_value('whole blood', 'Whole Blood', 'substring')

    def test_no_match(self):
        assert not match_value('PBMC', 'whole blood', 'substring')

    def test_empty_raw(self):
        assert not match_value('', 'blood', 'substring')


class TestMatchValueGlob:
    def test_wildcard_suffix(self):
        assert match_value('brain cortex', 'brain*', 'glob')

    def test_wildcard_prefix(self):
        assert match_value('frontal cortex', '*cortex', 'glob')

    def test_case_insensitive(self):
        assert match_value('Brain Cortex', 'brain*', 'glob')

    def test_no_match(self):
        assert not match_value('liver', 'brain*', 'glob')

    def test_exact_via_glob(self):
        assert match_value('blood', 'blood', 'glob')


class TestMatchValueRegex:
    def test_simple_pattern(self):
        assert match_value('GSM123456', r'GSM\d+', 'regex')

    def test_alternation(self):
        assert match_value('pbmc', r'pbmc|peripheral blood mononuclear', 'regex')
        assert match_value('peripheral blood mononuclear cells', r'pbmc|peripheral blood mononuclear', 'regex')

    def test_case_insensitive(self):
        assert match_value('PBMC', r'pbmc', 'regex')

    def test_no_match(self):
        assert not match_value('liver', r'^blood', 'regex')


def test_match_value_unknown_rule_type_raises():
    with pytest.raises(ValueError, match='Unknown rule_type'):
        match_value('foo', 'bar', 'exact')


# ---------------------------------------------------------------------------
# first_matching_rule
# ---------------------------------------------------------------------------


def _rule(id, pattern, rule_type='substring', priority=0, target='tissue', value='blood'):
    return {
        'id': id,
        'source_attribute': 'tissue',
        'pattern': pattern,
        'rule_type': rule_type,
        'target_attribute': target,
        'attribute_value': value,
        'priority': priority,
    }


class TestFirstMatchingRule:
    def test_returns_first_match(self):
        rules = [_rule(1, 'blood', value='blood'), _rule(2, 'pbmc', value='pbmc')]
        result = first_matching_rule('whole blood', rules)
        assert result['id'] == 1

    def test_skips_non_matching(self):
        rules = [_rule(1, 'brain'), _rule(2, 'blood')]
        result = first_matching_rule('whole blood', rules)
        assert result['id'] == 2

    def test_returns_none_on_no_match(self):
        rules = [_rule(1, 'brain'), _rule(2, 'liver')]
        assert first_matching_rule('blood', rules) is None

    def test_empty_rules(self):
        assert first_matching_rule('blood', []) is None

    def test_priority_order_respected(self):
        # Caller is responsible for sorting; first_matching_rule takes list as-is.
        # High-priority rule is first → it wins even if low-priority also matches.
        rules = [
            _rule(10, 'blood', value='high-priority'),
            _rule(5, 'blood', value='low-priority'),
        ]
        result = first_matching_rule('blood', rules)
        assert result['attribute_value'] == 'high-priority'


# ---------------------------------------------------------------------------
# apply_rules_to_sample
# ---------------------------------------------------------------------------


def _make_rules(*specs):
    """Helper: each spec is (source, pattern, rule_type, target, value, priority)."""
    return [
        {
            'id': i + 1,
            'source_attribute': src,
            'pattern': pat,
            'rule_type': rt,
            'target_attribute': tgt,
            'attribute_value': val,
            'priority': pri,
        }
        for i, (src, pat, rt, tgt, val, pri) in enumerate(specs)
    ]


class TestApplyRulesToSample:
    def test_structured_field_match(self):
        sample = {'tissue': 'whole blood', 'extras': {}}
        rules = _make_rules(('tissue', 'blood', 'substring', 'tissue', 'blood', 0))
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {'tissue': 'blood'}

    def test_extras_field_match(self):
        sample = {'tissue': None, 'extras': {'cell type': 'PBMC'}}
        rules = _make_rules(('cell type', 'pbmc', 'substring', 'tissue', 'blood', 0))
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {'tissue': 'blood'}

    def test_no_match_returns_empty(self):
        sample = {'tissue': 'liver', 'extras': {}}
        rules = _make_rules(('tissue', 'blood', 'substring', 'tissue', 'blood', 0))
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {}

    def test_missing_source_value_skipped(self):
        sample = {'tissue': None, 'extras': {}}
        rules = _make_rules(('tissue', 'blood', 'substring', 'tissue', 'blood', 0))
        assert apply_rules_to_sample(sample, rules) == {}

    def test_empty_string_source_skipped(self):
        sample = {'tissue': '', 'extras': {}}
        rules = _make_rules(('tissue', '', 'substring', 'tissue', 'blood', 0))
        assert apply_rules_to_sample(sample, rules) == {}

    def test_priority_sorting_applied(self):
        sample = {'tissue': 'whole blood'}
        rules = _make_rules(
            ('tissue', 'blood', 'substring', 'tissue', 'low', 0),
            ('tissue', 'blood', 'substring', 'tissue', 'high', 10),
        )
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {'tissue': 'high'}

    def test_gender_value_passed_through(self):
        # apply_rules_to_sample doesn't cast — server.py handles ::gender cast.
        sample = {'gender': 'M', 'extras': {}}
        rules = _make_rules(('gender', '^m', 'regex', 'gender', 'male', 0))
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {'gender': 'male'}

    def test_multiple_source_attributes(self):
        sample = {'tissue': 'blood', 'disease': 'type 2 diabetes', 'extras': {}}
        rules = _make_rules(
            ('tissue', 'blood', 'substring', 'tissue', 'blood', 0),
            ('disease', 'diabetes', 'substring', 'disease', 'diabetes mellitus', 0),
        )
        changes = apply_rules_to_sample(sample, rules)
        assert changes == {'tissue': 'blood', 'disease': 'diabetes mellitus'}

    def test_no_extras_key_missing(self):
        # Sample with no 'extras' key at all should not raise.
        sample = {'tissue': 'blood'}
        rules = _make_rules(('cell type', 'pbmc', 'substring', 'tissue', 'blood', 0))
        assert apply_rules_to_sample(sample, rules) == {}

    def test_structured_fields_constant(self):
        assert 'tissue' in STRUCTURED_FIELDS
        assert 'disease' in STRUCTURED_FIELDS
        assert 'gender' in STRUCTURED_FIELDS
        assert 'age' in STRUCTURED_FIELDS
        assert 'extraction_protocol' in STRUCTURED_FIELDS
