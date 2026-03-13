"""
Flask webapp for managing attribute normalisation rules.

Run with:
    uv run flask --app miqa.server run [--debug]
"""

import json
import re

import psycopg
from flask import Flask, jsonify, render_template, request

import miqa.config as config
import miqa.normalise as norm

app = Flask(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def get_conn():
    return psycopg.connect(config.DATABASE_URL)


def _fetch_rules(conn, target: str | None = None) -> list[dict]:
    """Return all rules, optionally filtered by target_attribute."""
    if target:
        rows = conn.execute(
            'SELECT rule_id, source_attribute, pattern, rule_type, target_attribute,'
            '       attribute_value, priority, created_at'
            '  FROM normalisation_rule'
            ' WHERE target_attribute = %s'
            ' ORDER BY priority DESC, rule_id',
            (target,),
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT rule_id, source_attribute, pattern, rule_type, target_attribute,'
            '       attribute_value, priority, created_at'
            '  FROM normalisation_rule'
            ' ORDER BY priority DESC, rule_id',
        ).fetchall()
    cols = [
        'rule_id',
        'source_attribute',
        'pattern',
        'rule_type',
        'target_attribute',
        'attribute_value',
        'priority',
        'created_at',
    ]
    return [dict(zip(cols, row)) for row in rows]


def _fetch_all_samples(conn) -> list[dict]:
    rows = conn.execute('SELECT id, source_metadata FROM sample').fetchall()
    return [{'id': r[0], 'source_metadata': r[1] or {}} for r in rows]


def _update_sample(conn, sample_id: int, changes: dict) -> None:
    """Merge rule-derived changes into normalised_metadata."""
    conn.execute(
        "UPDATE sample SET normalised_metadata = coalesce(normalised_metadata, '{}') || %s"
        ' WHERE id = %s',
        (json.dumps(changes), sample_id),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get('/')
def index():
    with get_conn() as conn:
        rules = _fetch_rules(conn)
        preview_rows = _build_preview(conn, source_attr='tissue', limit=50)
    return render_template(
        'index.html', rules=rules, preview_rows=preview_rows, preview_source='tissue'
    )


@app.get('/rules')
def list_rules():
    target = request.args.get('target') or None
    with get_conn() as conn:
        rules = _fetch_rules(conn, target=target)
    return render_template('partials/rule_list.html', rules=rules, filter_target=target or '')


@app.post('/rules')
def create_rule():
    data = request.form
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO normalisation_rule'
            '  (source_attribute, pattern, rule_type, target_attribute, attribute_value, priority)'
            ' VALUES (%s, %s, %s, %s, %s, %s)',
            (
                data['source_attribute'],
                data['pattern'],
                data['rule_type'],
                data['target_attribute'],
                data['attribute_value'],
                int(data.get('priority', 0)),
            ),
        )
        rules = _fetch_rules(conn)
    return render_template('partials/rule_list.html', rules=rules, filter_target='')


@app.route('/rules/<int:rule_id>', methods=['PUT'])
def update_rule(rule_id: int):
    data = request.form
    with get_conn() as conn:
        conn.execute(
            'UPDATE normalisation_rule'
            '   SET source_attribute = %s,'
            '       pattern          = %s,'
            '       rule_type        = %s,'
            '       target_attribute = %s,'
            '       attribute_value  = %s,'
            '       priority         = %s'
            ' WHERE id = %s',
            (
                data['source_attribute'],
                data['pattern'],
                data['rule_type'],
                data['target_attribute'],
                data['attribute_value'],
                int(data.get('priority', 0)),
                rule_id,
            ),
        )
        rules = _fetch_rules(conn)
    return render_template('partials/rule_list.html', rules=rules, filter_target='')


@app.route('/rules/<int:rule_id>', methods=['DELETE'])
def delete_rule(rule_id: int):
    with get_conn() as conn:
        conn.execute('DELETE FROM normalisation_rule WHERE id = %s', (rule_id,))
        rules = _fetch_rules(conn)
    return render_template('partials/rule_list.html', rules=rules, filter_target='')


@app.get('/preview')
def preview():
    source_attr = request.args.get('source', 'tissue')
    limit = int(request.args.get('limit', 50))
    ids_raw = request.args.get('ids', '').strip()
    sample_ids = [s.strip() for s in ids_raw.split(',') if s.strip()] if ids_raw else None
    with get_conn() as conn:
        rows = _build_preview(conn, source_attr=source_attr, limit=limit, sample_ids=sample_ids)
        rules = _fetch_rules(conn)
    return render_template(
        'partials/preview_table.html', preview_rows=rows, preview_source=source_attr, rules=rules
    )


@app.get('/sample/inspect')
def sample_inspect():
    sample_id = request.args.get('id', '').strip()
    if not sample_id:
        return jsonify(None), 400
    with get_conn() as conn:
        row = conn.execute(
            'SELECT source_metadata FROM sample WHERE repository_sample_id = %s LIMIT 1',
            (sample_id,),
        ).fetchone()
    if row is None:
        return jsonify(None), 404
    return jsonify(row[0] or {})


@app.post('/apply')
def apply_rules():
    with get_conn() as conn:
        rules = _fetch_rules(conn)
        samples = _fetch_all_samples(conn)
        updated = 0
        for sample in samples:
            changes = norm.apply_rules_to_sample(sample, rules)
            if changes:
                _update_sample(conn, sample['id'], changes)
                updated += 1
    return jsonify({'updated': updated})


# ---------------------------------------------------------------------------
# Preview helper
# ---------------------------------------------------------------------------


def _build_preview(
    conn, source_attr: str, limit: int, sample_ids: list[str] | None = None
) -> list[dict]:
    """Fetch up to *limit* samples and apply rules to produce preview rows.

    If *sample_ids* is provided, only rows whose ``repository_sample_id`` is in
    the list are returned (the *limit* still applies).
    """
    rules = _fetch_rules(conn)
    source_rules = [r for r in rules if r['source_attribute'] == source_attr]

    id_filter = ' AND repository_sample_id = ANY(%s)' if sample_ids else ''
    id_param = [sample_ids] if sample_ids else []

    rows = conn.execute(
        'SELECT id, repository_sample_id, source_metadata->>%s FROM sample'
        f' WHERE source_metadata ? %s{id_filter} LIMIT %s',
        (source_attr, source_attr, *id_param, limit),
    ).fetchall()

    preview = []
    for row in rows:
        sample_id, repo_sample_id, raw_value = row
        if raw_value is None:
            continue
        matched = None
        for rule in sorted(source_rules, key=lambda r: r['priority'], reverse=True):
            if norm.match_value(str(raw_value), rule['pattern'], rule['rule_type']):
                matched = rule
                break

        preview.append(
            {
                'sample_id': repo_sample_id,
                'raw_value': raw_value,
                'matched_rule': matched,
                'result': matched['attribute_value'] if matched else None,
            }
        )

    return preview


# ---------------------------------------------------------------------------
# Stats page
# ---------------------------------------------------------------------------

# Attributes for which rule-based mapping makes sense (excludes gender, age).
_MAPPABLE_ATTRS = ['tissue', 'disease', 'extraction_protocol']


@app.get('/stats')
def stats():
    with get_conn() as conn:
        data = _compute_stats(conn)
    return render_template('stats.html', **data)


def _compute_stats(conn) -> dict:
    total = conn.execute('SELECT COUNT(*) FROM sample').fetchone()[0]
    rules = _fetch_rules(conn)

    # Pre-group rules by source_attribute, sorted by priority desc.
    rules_by_source: dict[str, list[dict]] = {}
    for r in rules:
        rules_by_source.setdefault(r['source_attribute'], []).append(r)
    for lst in rules_by_source.values():
        lst.sort(key=lambda r: r['priority'], reverse=True)

    rule_hits: dict[int, int] = {r['rule_id']: 0 for r in rules}

    coverage = []
    distributions = {}  # attr -> [(canonical_value, count), ...]
    unmapped_values = {}  # attr -> [(raw_value, count), ...]  top-20 unmapped

    for attr in _MAPPABLE_ATTRS:
        freq_rows = conn.execute(
            'SELECT source_metadata->>%s, COUNT(*) FROM sample'
            ' WHERE source_metadata ? %s'
            ' GROUP BY source_metadata->>%s'
            ' ORDER BY COUNT(*) DESC',
            (attr, attr, attr),
        ).fetchall()

        has_value = sum(cnt for _, cnt in freq_rows)
        attr_rules = rules_by_source.get(attr, [])

        canonical_counts: dict[str, int] = {}
        unmapped: dict[str, int] = {}

        for raw_val, cnt in freq_rows:
            matched = norm.first_matching_rule(raw_val, attr_rules)
            if matched:
                rule_hits[matched['rule_id']] = rule_hits.get(matched['rule_id'], 0) + cnt
                canonical_counts[matched['attribute_value']] = (
                    canonical_counts.get(matched['attribute_value'], 0) + cnt
                )
            else:
                unmapped[raw_val] = cnt

        mapped = sum(canonical_counts.values())
        coverage.append(
            {
                'attribute': attr,
                'total': total,
                'has_value': has_value,
                'mapped': mapped,
                'unmapped': has_value - mapped,
                'pct_covered': round(mapped / total * 100, 1) if total else 0,
            }
        )
        distributions[attr] = sorted(canonical_counts.items(), key=lambda x: x[1], reverse=True)
        unmapped_values[attr] = sorted(unmapped.items(), key=lambda x: x[1], reverse=True)[:20]

    # Gender — read from source_metadata; no rule simulation needed.
    gender_rows = conn.execute(
        "SELECT source_metadata->>'gender', COUNT(*) FROM sample"
        " GROUP BY source_metadata->>'gender'",
    ).fetchall()
    gender_counts = {(g if g is not None else 'not_recorded'): c for g, c in gender_rows}
    gender_has_value = sum(c for g, c in gender_counts.items() if g is not None)
    coverage.append(
        {
            'attribute': 'gender',
            'total': total,
            'has_value': gender_has_value,
            'mapped': None,
            'unmapped': None,
            'pct_covered': None,
        }
    )

    # Age — numeric histogram.
    age_rows = conn.execute(
        "SELECT source_metadata->>'age' FROM sample WHERE source_metadata ? 'age'",
    ).fetchall()
    age_values = [r[0] for r in age_rows]
    age_histogram = _build_age_histogram(age_values)
    coverage.append(
        {
            'attribute': 'age',
            'total': total,
            'has_value': len(age_values),
            'mapped': None,
            'unmapped': None,
            'pct_covered': None,
        }
    )

    # Rule effectiveness — sorted by hit count descending.
    rule_effectiveness = sorted(
        [{'rule': r, 'hits': rule_hits.get(r['rule_id'], 0)} for r in rules],
        key=lambda x: x['hits'],
        reverse=True,
    )

    # Platform × repository breakdown for tissue (most biologically relevant).
    tissue_rules = rules_by_source.get('tissue', [])
    pb_rows = conn.execute(
        "SELECT source_metadata->>'tissue', platform_id, repository_id, COUNT(*)"
        '  FROM sample'
        "  WHERE source_metadata ? 'tissue'"
        "  GROUP BY source_metadata->>'tissue', platform_id, repository_id",
    ).fetchall()
    platform_breakdown: dict[str, dict[str, int]] = {}
    for raw_tissue, platform_id, repo_id, cnt in pb_rows:
        matched = norm.first_matching_rule(raw_tissue, tissue_rules)
        canonical = matched['attribute_value'] if matched else f'(unmapped) {raw_tissue}'
        label = f'{repo_id or "?"} / {platform_id or "?"}'
        platform_breakdown.setdefault(canonical, {})
        platform_breakdown[canonical][label] = platform_breakdown[canonical].get(label, 0) + cnt
    # Sort canonical tissue by total count descending.
    platform_breakdown = dict(
        sorted(
            platform_breakdown.items(),
            key=lambda kv: sum(kv[1].values()),
            reverse=True,
        )
    )

    return {
        'total_samples': total,
        'coverage': coverage,
        'distributions': distributions,
        'unmapped_values': unmapped_values,
        'rule_effectiveness': rule_effectiveness,
        'gender_counts': gender_counts,
        'age_histogram': age_histogram,
        'platform_breakdown': platform_breakdown,
    }


def _build_age_histogram(age_values: list[str]) -> list[dict]:
    """Bucket age strings into 10-year intervals. Non-numeric values are counted separately."""
    buckets: dict[int, int] = {}
    non_numeric = 0

    for val in age_values:
        m = re.search(r'\d+', val)
        if m:
            bucket = (int(m.group()) // 10) * 10
            buckets[bucket] = buckets.get(bucket, 0) + 1
        else:
            non_numeric += 1

    result = [
        {'label': f'{b}–{b + 9}', 'bucket': b, 'count': c} for b, c in sorted(buckets.items())
    ]
    if non_numeric:
        result.append({'label': 'non-numeric', 'bucket': 9999, 'count': non_numeric})
    return result
