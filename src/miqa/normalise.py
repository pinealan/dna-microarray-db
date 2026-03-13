"""
Pure rule-evaluation logic for normalisation rules.

No database or HTTP I/O — all functions accept plain dicts and return plain
values, making them fully unit-testable without any external dependencies.
"""

import re

STRUCTURED_FIELDS = {'tissue', 'disease', 'gender', 'age', 'extraction_protocol'}


def match_value(src_attr_value: str, pattern: str, rule_type: str) -> bool:
    """Return True if *raw* matches *pattern* according to *rule_type*.

    rule_type must be one of 'exact', 'substring', or 'regex'.
    All comparisons are case-insensitive.
    """

    # Make sure this set is in-sync with what's allowed in `rule_type` table in the DB
    if rule_type == 'verbatim':
        return True
    if rule_type == 'exact':
        return pattern.lower() == src_attr_value.lower()
    if rule_type == 'substring':
        return pattern.lower() in src_attr_value.lower()
    if rule_type == 'regex':
        return bool(re.search(pattern, src_attr_value, re.IGNORECASE))
    raise ValueError(f'Unknown rule_type: {rule_type!r}')


def first_matching_rule(src_attr_value: str, rules: list[dict]) -> dict | None:
    """Return the first rule in *rules* that matches *raw*, or None.

    *rules* must already be sorted by priority descending (highest first).
    The first match wins — subsequent rules are not evaluated.
    All comparisons are case-insensitive.
    """
    for rule in rules:
        if match_value(src_attr_value, rule['pattern'], rule['rule_type']):
            return rule
    return None


def apply_rules_to_sample(sample: dict, rules: list[dict]) -> dict:
    """Apply *rules* to *sample* and return a dict of proposed changes.

    Returns a mapping of ``{target_attribute: attribute_value}`` for every
    source/target pair where at least one rule matched the raw value.

    *rules* may contain rules for multiple source attributes; they are grouped
    internally. Each group is sorted by priority descending before evaluation.

    Args:
        sample: Dict with a ``source_metadata`` key holding the raw crawler
            metadata as a nested dict.
        rules: List of rule dicts, each with keys:
            rule_id, source_attribute, pattern, rule_type,
            target_attribute, attribute_value, priority.

    Returns:
        Dict of ``{target_attribute: new_value}`` intended to be merged into
        ``normalised_metadata`` — may be empty if no rules matched.
    """
    # Group rules by source_attribute, preserving priority order within each group.
    groups: dict[str, list[dict]] = {}
    for rule in rules:
        groups.setdefault(rule['source_attribute'], []).append(rule)

    changes: dict[str, str] = {}
    for source_attr, group in groups.items():
        # Sort descending by priority so first_matching_rule picks the highest.
        sorted_group = sorted(group, key=lambda r: r['priority'], reverse=True)
        src_attr_value = (sample.get('source_metadata') or {}).get(source_attr)
        if not src_attr_value:
            continue
        matched = first_matching_rule(src_attr_value, sorted_group)
        if matched:
            if matched['rule_type'] == 'verbatim':
                changes[matched['target_attribute']] = src_attr_value
            else:
                changes[matched['target_attribute']] = matched['attribute_value']

    return changes
