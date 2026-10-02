"""Diagnostics never export a camera's Local RTMP stream URL."""

from __future__ import annotations

from homeassistant.components.diagnostics import async_redact_data

from custom_components.nanit.diagnostics import TO_REDACT


def test_local_rtmp_urls_are_redacted() -> None:
    options = {"local_rtmp_urls": {"N301X": "rtmp://192.168.0.250:1935/nursery_s3cret"}}

    redacted = async_redact_data(options, TO_REDACT)

    assert "s3cret" not in str(redacted)
    assert "N301X" not in str(redacted)
