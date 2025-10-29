from app.core.config import Settings


def test_settings_defaults():
    settings = Settings(_env_file=None)

    assert settings.PROJECT_NAME == "Fund Performance Analysis System"
    assert settings.MAX_UPLOAD_SIZE == 50 * 1024 * 1024
    assert settings.DOCUMENT_PROCESSOR_USE_DOCLING is True


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("DOCUMENT_PROCESSOR_USE_DOCLING", "false")
    monkeypatch.setenv("MAX_UPLOAD_SIZE", "1024")

    settings = Settings(_env_file=None)

    assert settings.DOCUMENT_PROCESSOR_USE_DOCLING is False
    assert settings.MAX_UPLOAD_SIZE == 1024
