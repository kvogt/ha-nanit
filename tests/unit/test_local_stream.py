"""Tests for the LAN RTMP push keeper."""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.nanit.local_stream import (
    LOCAL_STREAM_INTERVAL,
    LocalStreamKeeper,
    is_valid_local_rtmp_url,
    redact_url,
)

URL = "rtmp://192.168.0.250:1935/nursery_camera_s3cret"


async def _resolve_hass(hass: Any) -> HomeAssistant:
    if hasattr(hass, "__anext__"):
        return await hass.__anext__()
    return cast(HomeAssistant, hass)


def _camera(*, connected: bool = True, fail: bool = False) -> MagicMock:
    camera = MagicMock(uid="cam_1")
    camera.connected = connected
    camera.async_start_streaming = AsyncMock(
        side_effect=RuntimeError("ack timeout") if fail else None
    )
    return camera


@pytest.mark.parametrize(
    ("url", "valid"),
    [
        (URL, True),
        ("rtmps://media.local/live/cam", True),
        ("rtmp://192.168.0.250:1935/", False),
        ("rtmp:///cam", False),
        ("http://192.168.0.250/cam", False),
        ("192.168.0.250:1935/cam", False),
        ("rtmp://192.168.0.250:65536/cam", False),
        ("rtmp://192.168.0.250:0/cam", False),
        ("rtmp://192.168.0.250:abc/cam", False),
        ("rtmp://192.168.0.250:-1/cam", False),
        ("rtmp://192.168.0.250:65535/cam", True),
        ("", False),
    ],
)
def test_is_valid_local_rtmp_url(url: str, valid: bool) -> None:
    assert is_valid_local_rtmp_url(url) is valid


def test_redact_url_hides_stream_path() -> None:
    assert redact_url(URL) == "rtmp://192.168.0.250:1935/…"
    assert "s3cret" not in redact_url(URL)


async def test_start_requests_push_with_lan_url(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera()
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)

    keeper.start()
    await hass.async_block_till_done()

    camera.async_start_streaming.assert_awaited_once_with(rtmps_url=URL, reconnect_on_failure=False)
    keeper.stop()


async def test_interval_resends_until_stopped(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera()
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)
    keeper.start()
    await hass.async_block_till_done()

    async_fire_time_changed(hass, dt_util.utcnow() + LOCAL_STREAM_INTERVAL)
    await hass.async_block_till_done()
    assert camera.async_start_streaming.await_count == 2

    keeper.stop()
    async_fire_time_changed(hass, dt_util.utcnow() + LOCAL_STREAM_INTERVAL * 3)
    await hass.async_block_till_done()
    assert camera.async_start_streaming.await_count == 2


async def test_no_request_while_camera_sleeps(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera()
    awake = False
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: awake)

    keeper.start()
    await hass.async_block_till_done()
    camera.async_start_streaming.assert_not_awaited()

    awake = True
    keeper.request()
    await hass.async_block_till_done()
    camera.async_start_streaming.assert_awaited_once()
    keeper.stop()


async def test_no_request_while_disconnected(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera(connected=False)
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)

    keeper.start()
    await hass.async_block_till_done()

    camera.async_start_streaming.assert_not_awaited()
    keeper.stop()


async def test_request_before_start_or_after_stop_is_ignored(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera()
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)

    keeper.request()
    await hass.async_block_till_done()
    keeper.start()
    keeper.stop()
    keeper.request()
    await hass.async_block_till_done()

    assert camera.async_start_streaming.await_count <= 1


async def test_failed_request_is_swallowed_and_retried(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera(fail=True)
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)

    keeper.start()
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + LOCAL_STREAM_INTERVAL)
    await hass.async_block_till_done()

    assert camera.async_start_streaming.await_count == 2
    keeper.stop()


async def test_cancel_pending_cancels_in_flight_send(hass: HomeAssistant) -> None:
    hass = await _resolve_hass(hass)
    camera = _camera()
    started = asyncio.Event()

    async def slow_send(**_kwargs: Any) -> None:
        started.set()
        await asyncio.sleep(3600)

    camera.async_start_streaming = AsyncMock(side_effect=slow_send)
    keeper = LocalStreamKeeper(hass, camera, URL, lambda: True)
    keeper.start()
    await started.wait()
    task = keeper._task
    assert task is not None
    assert not task.done()

    keeper.cancel_pending()
    await asyncio.sleep(0)

    assert task.done()
    keeper.stop()
