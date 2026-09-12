"""Real SQLite ownership regressions; all files live in pytest's temporary root."""

from __future__ import annotations

import sqlite3

import pytest

from corpusledger.annotation_store import AnnotationEvent, AnnotationStore, AnnotationStoreError, _json
from corpusledger.annotations import AnnotationDocument


@pytest.fixture
def store(tmp_path):
    with AnnotationStore(tmp_path / "owned.sqlite", timeout=0) as value:
        yield value


def insert_marker(connection):
    document = AnnotationDocument("caller", "original synthetic transaction marker")
    connection.execute("INSERT INTO documents(digest,body) VALUES(?,?)", (document.digest, _json(document.to_dict())))
    return document.digest


def visible_markers(path):
    with sqlite3.connect(path, isolation_level=None, timeout=0) as observer:
        return observer.execute("SELECT COUNT(*) FROM documents").fetchone()[0]


@pytest.mark.parametrize("outer_begin", ["BEGIN", "BEGIN IMMEDIATE"])
@pytest.mark.parametrize("nested_write", [False, True])
def test_nested_entry_does_not_rollback_or_commit_callers_pending_insert(store, outer_begin, nested_write):
    connection = store._connection
    connection.execute(outer_begin)
    marker = insert_marker(connection)
    assert visible_markers(store.path) == 0
    statements = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(AnnotationStoreError), store._transaction(write=nested_write):
            pytest.fail("nested transaction body must never be entered")
        # Original implementation rolls back the existing transaction here.
        assert connection.in_transaction
        assert connection.execute("SELECT digest FROM documents").fetchone() == (marker,)
        assert visible_markers(store.path) == 0
        assert not any(sql.upper().startswith(("BEGIN", "COMMIT", "ROLLBACK")) for sql in statements)
        connection.execute("COMMIT")
        assert visible_markers(store.path) == 1
    finally:
        connection.set_trace_callback(None)
        if connection.in_transaction:
            connection.execute("ROLLBACK")


@pytest.mark.parametrize("action", ["get", "put", "upgrade"])
def test_public_methods_reject_nesting_without_destroying_callers_work(store, action):
    with store._transaction(write=True):
        marker = insert_marker(store._connection)
        with pytest.raises(AnnotationStoreError):
            if action == "get":
                store.get("missing")
            elif action == "put":
                store.put(AnnotationEvent("new"))
            else:
                store.enable_execution_journal()
        assert store._connection.in_transaction
        assert store._connection.execute("SELECT digest FROM documents").fetchone() == (marker,)
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert store._connection.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == 0
    assert visible_markers(store.path) == 1


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_owned_business_failure_still_rolls_back_exact_original_exception(store, error_type):
    error = error_type("authored failure")
    with pytest.raises(error_type) as caught, store._transaction(write=True):
        insert_marker(store._connection)
        raise error
    assert caught.value is error
    assert not store._connection.in_transaction
    assert visible_markers(store.path) == 0


def test_owned_success_commits_and_releases_scope(store):
    with store._transaction(write=True):
        insert_marker(store._connection)
        assert visible_markers(store.path) == 0
    assert not store._connection.in_transaction
    assert visible_markers(store.path) == 1
    with store._transaction():
        assert store._connection.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 1


def test_denied_begin_never_attempts_rollback_and_does_not_enter_body(store):
    transactions = []

    def authorizer(action, name, _value, _database, _source):
        if action == sqlite3.SQLITE_TRANSACTION:
            transactions.append(name)
            if name == "BEGIN":
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    store._connection.set_authorizer(authorizer)
    try:
        with pytest.raises(AnnotationStoreError, match="transaction failed"), store._transaction(write=True):
            pytest.fail("denied BEGIN must not acquire a scope")
        assert transactions == ["BEGIN"]
        assert not store._connection.in_transaction
    finally:
        store._connection.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
    assert visible_markers(store.path) == 0


def test_busy_begin_does_not_affect_independent_writer(store):
    with sqlite3.connect(store.path, isolation_level=None, timeout=0) as other:
        other.execute("BEGIN IMMEDIATE")
        marker = insert_marker(other)
        with pytest.raises(AnnotationStoreError, match="transaction failed"), store._transaction(write=True):
            pytest.fail("busy writer must not acquire a scope")
        assert not store._connection.in_transaction
        assert other.in_transaction
        assert other.execute("SELECT digest FROM documents").fetchone() == (marker,)
        assert visible_markers(store.path) == 0
        other.execute("COMMIT")
    assert visible_markers(store.path) == 1


def test_failed_commit_rolls_back_the_transaction_it_did_acquire(store):
    transactions = []

    def authorizer(action, name, _value, _database, _source):
        if action == sqlite3.SQLITE_TRANSACTION:
            transactions.append(name)
            if name == "COMMIT":
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    store._connection.set_authorizer(authorizer)
    try:
        with pytest.raises(AnnotationStoreError, match="transaction failed"), store._transaction(write=True):
            insert_marker(store._connection)
        assert transactions == ["BEGIN", "COMMIT", "ROLLBACK"]
        assert not store._connection.in_transaction
    finally:
        store._connection.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
    assert visible_markers(store.path) == 0


@pytest.mark.parametrize("close_method", ["store", "connection"])
def test_closed_before_entry_reports_store_error_without_entering_body(store, close_method):
    if close_method == "store":
        store.close()
    else:
        store._connection.close()
    with pytest.raises(AnnotationStoreError), store._transaction(write=True):
        pytest.fail("closed store must not acquire a scope")
    assert visible_markers(store.path) == 0


@pytest.mark.parametrize("close_method", ["store", "connection"])
def test_connection_closed_inside_owned_scope_does_not_claim_commit(store, close_method):
    with pytest.raises(AnnotationStoreError), store._transaction(write=True):
        insert_marker(store._connection)
        if close_method == "store":
            store.close()
        else:
            store._connection.close()
    # SQLite close discards the pending insert; cleanup must not leak an unwrapped
    # ProgrammingError while trying to inspect the now-closed connection.
    assert visible_markers(store.path) == 0


def test_failed_rollback_is_reported_and_does_not_falsely_close_transaction(store):
    def authorizer(action, name, _value, _database, _source):
        return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and name == "ROLLBACK" else sqlite3.SQLITE_OK

    store._connection.set_authorizer(authorizer)
    try:
        with pytest.raises(AnnotationStoreError, match="rollback"), store._transaction(write=True):
            insert_marker(store._connection)
            raise RuntimeError("authored failure before refused cleanup")
        assert store._connection.in_transaction
        assert visible_markers(store.path) == 0
    finally:
        store._connection.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
        if store._connection.in_transaction:
            store._connection.execute("ROLLBACK")
    assert visible_markers(store.path) == 0
