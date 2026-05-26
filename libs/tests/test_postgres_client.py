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
    for var in ["SUPABASE_HOST", "SUPABASE_PORT", "SUPABASE_USER",
                "SUPABASE_PASSWORD", "SUPABASE_DATABASE", "SUPABASE_SSLMODE"]:
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
