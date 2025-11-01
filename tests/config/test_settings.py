"""Unit tests for configuration helpers."""

from opendt.config import settings


def test_openai_api_key_reads_environment(monkeypatch):
    """Ensure the OpenAI API key is sourced from environment variables."""

    monkeypatch.setenv("OPENAI_API_KEY", "env-key")

    assert settings.openai_api_key() == "env-key"
