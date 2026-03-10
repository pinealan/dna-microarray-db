"""
Pure rule-evaluation logic for normalisation rules.

No database or HTTP I/O — all functions accept plain dicts and return plain
values, making them fully unit-testable without any external dependencies.
"""

import fnmatch
import re

STRUCTURED_FIELDS = {'tissue', 'disease', 'gender', 'age', 'extraction_protocol'}


def match_value(raw: str, pattern: str, rule_type: str) -> bool:
    """Return True if *raw* matches *pattern* according to *rule_type*.

    rule_type must be one of 'substring', 'glob', or 'regex'.
    All comparisons are case-insensitive.
    """
    if rule_type == 'substring':
        return pattern.lower() in raw.lower()
    if rule_type == 'glob':
        return fnmatch.fnmatch(raw.lower(), pattern.lower())
    if rule_type == 'regex':
        return bool(re.search(pattern, raw, re.IGNORECASE))
    raise ValueError(f'Unknown rule_type: {rule_type!r}')


def first_matching_rule(raw: str, rules: list[dict]) -> dict | None:
    """Return the first rule in *rules* that matches *raw*, or None.

    *rules* must already be sorted by priority descending (highest first).
    The first match wins — subsequent rules are not evaluated.
    """
    for rule in rules:
        if match_value(raw, rule['pattern'], rule['rule_type']):
            return rule
    return None


def _get_raw_value(sample: dict, source_attribute: str) -> str | None:
    """Extract the raw value for *source_attribute* from a sample dict."""
    if source_attribute in STRUCTURED_FIELDS:
        return sample.get(source_attribute)
    extras = sample.get('extras') or {}
    return extras.get(source_attribute)


def apply_rules_to_sample(sample: dict, rules: list[dict]) -> dict:
    """Apply *rules* to *sample* and return a dict of proposed changes.

    Returns a mapping of ``{target_attribute: attribute_value}`` for every
    source/target pair where at least one rule matched the raw value.

    *rules* may contain rules for multiple source attributes; they are grouped
    internally. Each group is sorted by priority descending before evaluation.

    Args:
        sample: Dict with keys matching ``sample`` table columns plus an
            optional ``extras`` key holding a nested dict.
        rules: List of rule dicts, each with keys:
            id, source_attribute, pattern, rule_type,
            target_attribute, attribute_value, priority.

    Returns:
        Dict of ``{target_attribute: new_value}`` — may be empty if no rules
        matched any source values.
    """
    # Group rules by source_attribute, preserving priority order within each group.
    groups: dict[str, list[dict]] = {}
    for rule in rules:
        groups.setdefault(rule['source_attribute'], []).append(rule)

    changes: dict[str, str] = {}
    for source_attr, group in groups.items():
        # Sort descending by priority so first_matching_rule picks the highest.
        sorted_group = sorted(group, key=lambda r: r['priority'], reverse=True)
        raw = _get_raw_value(sample, source_attr)
        if not raw:
            continue
        matched = first_matching_rule(raw, sorted_group)
        if matched:
            changes[matched['target_attribute']] = matched['attribute_value']

    return changes
