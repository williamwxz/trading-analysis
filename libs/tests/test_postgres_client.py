"""Unit tests for libs.postgres_client.

These tests do not require a live Postgres — they verify the client builds
the correct connection string from environment variables.
"""

from unittest import mock

import pytest


@pytest.mark.unit
def test_get_client_uses_env_vars(monkeypatch):
    monkeypatch.setenv("SUPABASE_HOST", "db.example.supabase.co")
    monkeypatch.setenv("SUPABASE_PORT", "6543")
    monkeypatch.setenv("SUPABASE_USER", "postgres")
    monkeypatch.setenv("SUPABASE_PASSWORD", "secret")
    monkeypatch.setenv("SUPABASE_DATABASE", "postgres")
    monkeypatch.setenv("SUPABASE_SSLMODE", "require")

    fake_conn = mock.MagicMock()
    with mock.patch("psycopg.connect", return_value=fake_conn) as mock_connect:
        from libs.postgres_client import get_client

        client = get_client()

    assert client is fake_conn
    mock_connect.assert_called_once()
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["host"] == "db.example.supabase.co"
    assert kwargs["port"] == 6543
    assert kwargs["user"] == "postgres"
    assert kwargs["password"] == "secret"
    assert kwargs["dbname"] == "postgres"
    assert kwargs["sslmode"] == "require"


@pytest.mark.unit
def test_get_client_defaults(monkeypatch):
    for var in [
        "SUPABASE_HOST",
        "SUPABASE_PORT",
        "SUPABASE_USER",
        "SUPABASE_PASSWORD",
        "SUPABASE_DATABASE",
        "SUPABASE_SSLMODE",
    ]:
        monkeypatch.delenv(var, raising=False)

    fake_conn = mock.MagicMock()
    with mock.patch("psycopg.connect", return_value=fake_conn) as mock_connect:
        from libs.postgres_client import get_client

        get_client()

    kwargs = mock_connect.call_args.kwargs
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 5432
    assert kwargs["user"] == "postgres"
    assert kwargs["dbname"] == "postgres"
    assert kwargs["sslmode"] == "prefer"


@pytest.mark.unit
def test_query_rows_returns_lists(monkeypatch):
    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.fetchall.return_value = [(1, "a"), (2, "b")]
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    from libs.postgres_client import query_rows

    rows = query_rows("SELECT 1, 'a'", client=fake_conn)
    assert rows == [[1, "a"], [2, "b"]]


@pytest.mark.unit
def test_query_dicts_returns_dicts(monkeypatch):
    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.description = [("id", None), ("name", None)]
    fake_cur.fetchall.return_value = [(1, "a"), (2, "b")]
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    from libs.postgres_client import query_dicts

    rows = query_dicts("SELECT id, name FROM t", client=fake_conn)
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


@pytest.mark.unit
def test_transaction_context_commits_on_success(monkeypatch):
    fake_conn = mock.MagicMock()
    from libs.postgres_client import transaction

    with transaction(fake_conn) as conn:
        assert conn is fake_conn
    fake_conn.commit.assert_called_once()
    fake_conn.rollback.assert_not_called()


@pytest.mark.unit
def test_transaction_context_rolls_back_on_error(monkeypatch):
    fake_conn = mock.MagicMock()
    from libs.postgres_client import transaction

    with pytest.raises(RuntimeError, match="boom"):
        with transaction(fake_conn):
            raise RuntimeError("boom")
    fake_conn.commit.assert_not_called()
    fake_conn.rollback.assert_called_once()
