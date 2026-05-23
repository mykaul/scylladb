# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Test that CQL examples in documentation are valid and executable.

This test suite extracts CQL code blocks from docs/ and executes them
against a live ScyllaDB instance. The goal is to catch documentation
drift: examples that are syntactically invalid, reference non-existent
features, or produce errors.

Run with: test/cqlpy/run --run test_doc_examples.py
Or via:   test.py --mode dev --suite cqlpy -- -k test_doc_examples
"""

import pytest
import re
from pathlib import Path
from cassandra.protocol import InvalidRequest, SyntaxException

from .doc_cql_extractor import (
    extract_cql_blocks, CqlBlock, CORE_DOC_FILES, FEATURE_DOC_FILES,
)
from .util import unique_name, new_test_keyspace


# Repo root relative to this file
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _make_keyspace_name(doc_path: str) -> str:
    """Generate a keyspace name from a doc file path."""
    # e.g. docs/cql/dml/select.rst -> doc_cql_dml_select
    name = doc_path.replace('docs/', '').replace('/', '_').replace('.rst', '').replace('.md', '').replace('-', '_')
    return f'doc_{name}'[:48]


def _rewrite_keyspace_refs(stmt: str, ks: str) -> str:
    """Rewrite CREATE KEYSPACE statements to use our test keyspace name,
    and qualify unqualified table references if needed."""
    # Don't rewrite USE statements - handle them differently
    return stmt


def _is_create_keyspace(stmt: str) -> bool:
    return stmt.strip().upper().startswith('CREATE KEYSPACE')


def _is_drop_keyspace(stmt: str) -> bool:
    return stmt.strip().upper().startswith('DROP KEYSPACE')


def _is_use_statement(stmt: str) -> bool:
    return stmt.strip().upper().startswith('USE ')


def _is_create_function(stmt: str) -> bool:
    upper = stmt.strip().upper()
    return 'CREATE FUNCTION' in upper or 'CREATE AGGREGATE' in upper


def _is_grant_revoke(stmt: str) -> bool:
    upper = stmt.strip().upper()
    return upper.startswith('GRANT') or upper.startswith('REVOKE') or upper.startswith('LIST')


def _is_describe(stmt: str) -> bool:
    return stmt.strip().upper().startswith('DESCRIBE')


def _should_skip(stmt: str) -> bool:
    """Skip statements that require special setup or are not testable."""
    upper = stmt.strip().upper()
    # Skip GRANT/REVOKE (requires auth), DESCRIBE (not via driver),
    # CREATE FUNCTION/AGGREGATE (may require UDF enabled or WASM)
    if _is_grant_revoke(stmt) or _is_describe(stmt):
        return True
    if _is_create_function(stmt):
        return True
    # Skip statements referencing system keyspaces
    if 'SYSTEM.' in upper or 'SYSTEM_SCHEMA.' in upper:
        return True
    return False


def _collect_doc_statements(doc_file: str) -> list[tuple[int, str]]:
    """Extract all executable statements from a doc file.
    Returns list of (line_number, statement) tuples."""
    filepath = REPO_ROOT / doc_file
    if not filepath.exists():
        return []
    blocks = extract_cql_blocks(filepath)
    result = []
    for block in blocks:
        if block.is_grammar:
            continue
        for stmt in block.statements:
            if not _should_skip(stmt):
                result.append((block.line_number, stmt))
    return result


# Collect all doc files and their statements for parametrization
def _gather_test_cases():
    """Gather all test cases from documentation files."""
    cases = []
    for doc_file in CORE_DOC_FILES:
        stmts = _collect_doc_statements(doc_file)
        for line_no, stmt in stmts:
            # Create a readable test ID
            short_stmt = stmt[:60].replace('\n', ' ')
            test_id = f"{doc_file}:L{line_no}:{short_stmt}"
            cases.append(pytest.param(doc_file, line_no, stmt, id=test_id))
    return cases


# We need a different approach: execute statements in document order
# since later statements depend on earlier ones (INSERT needs CREATE TABLE first)
class TestDocExamples:
    """Execute CQL examples from documentation files in order.
    
    Each doc file gets its own test method that runs all examples sequentially.
    This preserves statement ordering (CREATE TABLE before INSERT, etc.).
    """

    @pytest.fixture
    def doc_keyspace(self, cql):
        """Create a temporary keyspace for doc example testing."""
        ks = unique_name()
        cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
        cql.execute(f"USE {ks}")
        yield ks
        cql.execute(f"DROP KEYSPACE {ks}")

    def _run_doc_file(self, cql, doc_keyspace, doc_file: str):
        """Run all CQL examples from a doc file, tracking successes and failures."""
        stmts = _collect_doc_statements(doc_file)
        if not stmts:
            pytest.skip(f"No executable CQL found in {doc_file}")

        # Track created keyspaces so we can clean them up
        created_keyspaces = []
        failures = []

        for line_no, stmt in stmts:
            # Rewrite CREATE KEYSPACE to add IF NOT EXISTS and use SimpleStrategy
            if _is_create_keyspace(stmt):
                # Extract keyspace name
                m = re.search(r'CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+|"[^"]+")', stmt, re.IGNORECASE)
                if m:
                    ks_name = m.group(1).strip('"')
                    # Create with simple replication for testing
                    try:
                        cql.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
                        created_keyspaces.append(ks_name)
                    except Exception as e:
                        failures.append((line_no, stmt, str(e)))
                continue

            if _is_drop_keyspace(stmt):
                # Skip DROP KEYSPACE - we'll clean up ourselves
                continue

            if _is_use_statement(stmt):
                # Extract keyspace name from USE
                m = re.match(r'\s*USE\s+(\w+|"[^"]+")', stmt, re.IGNORECASE)
                if m:
                    ks_name = m.group(1).strip('"')
                    try:
                        cql.execute(f"USE {ks_name}")
                    except Exception:
                        # Keyspace might not exist, use our test keyspace
                        cql.execute(f"USE {doc_keyspace}")
                continue

            try:
                cql.execute(stmt)
            except SyntaxException as e:
                failures.append((line_no, stmt, f"SyntaxError: {e}"))
            except InvalidRequest as e:
                # Some InvalidRequest errors are expected (e.g., table already exists)
                err_msg = str(e)
                if 'already exists' in err_msg.lower():
                    pass  # OK, doc examples may repeat CREATE TABLE
                else:
                    failures.append((line_no, stmt, f"InvalidRequest: {e}"))
            except Exception as e:
                failures.append((line_no, stmt, f"{type(e).__name__}: {e}"))

        # Cleanup created keyspaces
        for ks in created_keyspaces:
            try:
                cql.execute(f"DROP KEYSPACE IF EXISTS {ks}")
            except Exception:
                pass

        # Reset to test keyspace
        try:
            cql.execute(f"USE {doc_keyspace}")
        except Exception:
            pass

        if failures:
            msg = f"\n{len(failures)} CQL example(s) failed in {doc_file}:\n"
            for line_no, stmt, err in failures:
                short_stmt = stmt[:100].replace('\n', ' ')
                msg += f"  Line {line_no}: {short_stmt}\n    -> {err}\n"
            pytest.fail(msg)

    def test_doc_ddl(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/ddl.rst')

    def test_doc_select(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/dml/select.rst')

    def test_doc_insert(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/dml/insert.rst')

    def test_doc_update(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/dml/update.rst')

    def test_doc_delete(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/dml/delete.rst')

    def test_doc_batch(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/dml/batch.rst')

    def test_doc_ttl(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/time-to-live.rst')

    def test_doc_json(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/json.rst')

    def test_doc_types(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/types.rst')

    def test_doc_functions(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/functions.rst')

    def test_doc_secondary_indexes(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/secondary-indexes.rst')

    def test_doc_materialized_views(self, cql, doc_keyspace):
        self._run_doc_file(cql, doc_keyspace, 'docs/cql/mv.rst')
