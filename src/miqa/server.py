"""
Flask webapp for managing attribute normalisation rules.

Run with:
    uv run flask --app miqa.server run [--debug]
"""

import json
import os

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
    rows = conn.execute(
        'SELECT rule_id, tissue, disease, gender, age, extraction_protocol, extras  FROM sample',
    ).fetchall()
    cols = ['rule_id', 'tissue', 'disease', 'gender', 'age', 'extraction_protocol', 'extras']
    return [dict(zip(cols, row)) for row in rows]


def _update_sample(conn, sample_id: int, changes: dict) -> None:
    """Apply a dict of {column: value} changes to a single sample row."""
    for col, val in changes.items():
        if col == 'gender':
            conn.execute(
                'UPDATE sample SET gender = %s::gender WHERE id = %s',
                (val, sample_id),
            )
        elif col in norm.STRUCTURED_FIELDS:
            conn.execute(
                f'UPDATE sample SET {col} = %s WHERE id = %s',  # noqa: S608
                (val, sample_id),
            )
        else:
            # Write into extras JSONB
            conn.execute(
                'UPDATE sample'
                "   SET extras = jsonb_set(coalesce(extras, '{}'), %s, %s)"
                ' WHERE id = %s',
                ('{' + col + '}', json.dumps(val), sample_id),
            )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get('/')
def index():
    with get_conn() as conn:
        rules = _fetch_rules(conn)
        preview_rows = _build_preview(conn, source='tissue', limit=50)
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
    source = request.args.get('source', 'tissue')
    limit = int(request.args.get('limit', 50))
    ids_raw = request.args.get('ids', '').strip()
    sample_ids = [s.strip() for s in ids_raw.split(',') if s.strip()] if ids_raw else None
    with get_conn() as conn:
        rows = _build_preview(conn, source=source, limit=limit, sample_ids=sample_ids)
        rules = _fetch_rules(conn)
    return render_template(
        'partials/preview_table.html', preview_rows=rows, preview_source=source, rules=rules
    )


@app.post('/apply')
def apply_rules():
    with get_conn() as conn:
        rules = _fetch_rules(conn)
        samples = _fetch_all_samples(conn)
        updated = 0
        for sample in samples:
            changes = norm.apply_rules_to_sample(sample, rules)
            if changes:
                _update_sample(conn, sample['rule_id'], changes)
                updated += 1
    return jsonify({'updated': updated})


# ---------------------------------------------------------------------------
# Preview helper
# ---------------------------------------------------------------------------


def _build_preview(
    conn, source: str, limit: int, sample_ids: list[str] | None = None
) -> list[dict]:
    """Fetch up to *limit* samples and apply rules to produce preview rows.

    If *sample_ids* is provided, only rows whose ``repository_sample_id`` is in
    the list are returned (the *limit* still applies).
    """
    rules = _fetch_rules(conn)
    source_rules = [r for r in rules if r['source_attribute'] == source]

    id_filter = ' AND repository_sample_id = ANY(%s)' if sample_ids else ''
    id_param = [sample_ids] if sample_ids else []

    if source in norm.STRUCTURED_FIELDS:
        col = source
        rows = conn.execute(
            f'SELECT id, repository_sample_id, {col} FROM sample'  # noqa: S608
            f' WHERE {col} IS NOT NULL{id_filter} LIMIT %s',
            (*id_param, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            f'SELECT id, repository_sample_id, extras->%s FROM sample'  # noqa: S608
            f' WHERE extras ? %s{id_filter} LIMIT %s',
            (source, source, *id_param, limit),
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
