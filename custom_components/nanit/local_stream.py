"""Keep a camera's RTMP push to a LAN media server running.

A Nanit camera pushes RTMP to every URL it was sent in a PUT_STREAMING
request, alongside its push to Nanit's cloud ingest, so the official app
keeps working. The push lapses roughly 20 minutes after the last request
and can stop with a camera reboot or a control-session reconnect, so the
request is re-sent on an interval and on every reconnect. Re-sending the
same URL keeps the camera's existing RTMP connection.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import timedelta
from urllib.parse import urlsplit

from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval

from aionanit import NanitCamera

_LOGGER = logging.getLogger(__name__)

# Well inside the ~20 minute lapse; short so a push stopped by another
# client (a STOPPED request) is restored within a minute.
LOCAL_STREAM_INTERVAL = timedelta(seconds=60)


def is_valid_local_rtmp_url(url: str) -> bool:
    """Return True for an rtmp:// or rtmps:// URL with a host and a stream path."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    return parts.scheme in ("rtmp", "rtmps") and bool(parts.hostname) and len(parts.path) > 1


def redact_url(url: str) -> str:
    """Return scheme://host:port only; the stream path can be a secret."""
    try:
        parts = urlsplit(url)
        port = f":{parts.port}" if parts.port else ""
    except ValueError:
        return "<invalid url>"
    return f"{parts.scheme}://{parts.hostname}{port}/…"


class LocalStreamKeeper:
    """Re-send PUT_STREAMING with a fixed LAN URL while the camera is awake."""

    def __init__(
        self,
        hass: HomeAssistant,
        camera: NanitCamera,
        url: str,
        is_on: Callable[[], bool],
    ) -> None:
        """Initialize."""
        self._hass = hass
        self._camera = camera
        self._url = url
        self._is_on = is_on
        self._cancel_interval: CALLBACK_TYPE | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def url(self) -> str:
        """The LAN RTMP URL the camera is asked to push to."""
        return self._url

    @callback
    def start(self) -> None:
        """Request the push now and on every interval."""
        if self._cancel_interval is None:
            self._cancel_interval = async_track_time_interval(
                self._hass, self._handle_interval, LOCAL_STREAM_INTERVAL
            )
        self.request()

    @callback
    def stop(self) -> None:
        """Stop re-sending; the camera's push lapses on its own."""
        if self._cancel_interval is not None:
            self._cancel_interval()
            self._cancel_interval = None
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self._task = None

    @callback
    def _handle_interval(self, _now: object = None) -> None:
        self.request()

    @callback
    def request(self) -> None:
        """Send PUT_STREAMING once, unless a send is already in flight."""
        if self._cancel_interval is None:
            return
        if not self._is_on() or not self._camera.connected:
            return
        if self._task is not None and not self._task.done():
            return
        self._task = self._hass.async_create_task(
            self._async_send(), name=f"nanit_local_stream_{self._camera.uid}"
        )

    async def _async_send(self) -> None:
        try:
            # Best effort: forcing a control reconnect on a late ACK would
            # itself stop the camera's pushes.
            await self._camera.async_start_streaming(
                rtmps_url=self._url, reconnect_on_failure=False
            )
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "Local stream request to %s failed for camera %s: %s",
                redact_url(self._url),
                self._camera.uid,
                err,
            )
        else:
            _LOGGER.debug(
                "Local stream requested to %s for camera %s",
                redact_url(self._url),
                self._camera.uid,
            )
