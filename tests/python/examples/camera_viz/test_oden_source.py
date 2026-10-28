# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT
"""Tests for the Oden CUDA IPC texture export source (``type: oden``).

Colour-conversion and config tests run everywhere. The read tests stand in
for Oden with a GStreamer ``cudaupload ! cudaipcsink`` export, the same
protocol and NV12 layout Oden uses; they need cupy, a CUDA device and the
GStreamer CUDA elements, and skip cleanly without them.
"""

from __future__ import annotations

import shutil
import subprocess
import time

import pytest

from isaaccapture_examples.camera_viz.sources import build_local_camera, set_notify_sink
from isaaccapture_examples.camera_viz.sources.oden import (
    _caps_field,
    _ycbcr_coefficients,
)


def _to_rgb(colorimetry: str, y: int, cb: int, cr: int) -> tuple:
    """What the source's kernel computes for one NV12 sample."""
    y_offset, y_scale, cr_r, cb_g, cr_g, cb_b = _ycbcr_coefficients(colorimetry)
    yy = (y - y_offset) * y_scale
    rgb = (
        yy + cr_r * (cr - 128),
        yy + cb_g * (cb - 128) + cr_g * (cr - 128),
        yy + cb_b * (cb - 128),
    )
    return tuple(int(min(max(c + 0.5, 0), 255)) for c in rgb)


def _close(a, b, tol: int) -> bool:
    return all(abs(x - y) <= tol for x, y in zip(a, b))


@pytest.mark.parametrize("colorimetry", ["2:3:7:1", "bt709", ""])
def test_bt709_limited_range(colorimetry):
    assert _to_rgb(colorimetry, 235, 128, 128) == (255, 255, 255)
    assert _to_rgb(colorimetry, 16, 128, 128) == (0, 0, 0)
    assert _close(_to_rgb(colorimetry, 63, 102, 240), (255, 0, 0), 2)
    assert _close(_to_rgb(colorimetry, 173, 42, 26), (0, 255, 0), 2)


@pytest.mark.parametrize("colorimetry", ["jpeg", "1:4:0:0"])
def test_bt601_full_range(colorimetry):
    assert _to_rgb(colorimetry, 255, 128, 128) == (255, 255, 255)
    assert _to_rgb(colorimetry, 0, 128, 128) == (0, 0, 0)
    assert _close(_to_rgb(colorimetry, 76, 85, 255), (255, 0, 0), 2)


def test_caps_field():
    caps = (
        "video/x-raw(memory:CUDAMemory), format=(string)NV12, width=(int)1280, "
        "height=(int)720, colorimetry=(string)2:3:7:1"
    )
    assert _caps_field(caps, "width") == "1280"
    assert _caps_field(caps, "height") == "720"
    assert _caps_field(caps, "colorimetry") == "2:3:7:1"
    assert _caps_field(caps, "framerate") == ""


def test_stereo_needs_right_share_path():
    spec = {"name": "robot", "type": "oden", "width": 64, "height": 48, "stereo": True}
    with pytest.raises(ValueError, match="share_path_right"):
        build_local_camera(spec)


# ── Reading an export ─────────────────────────────────────────────────


def _cuda_available() -> bool:
    try:
        import cupy as cp
    except ImportError:
        return False
    try:
        return cp.cuda.runtime.getDeviceCount() > 0
    except Exception:
        return False


def _gst_cuda_ipc_available() -> bool:
    inspect = shutil.which("gst-inspect-1.0")
    if not shutil.which("gst-launch-1.0") or not inspect:
        return False
    return all(
        subprocess.run([inspect, e], capture_output=True, check=False).returncode == 0
        for e in ("cudaupload", "cudaipcsink", "cudaipcsrc")
    )


requires_export = pytest.mark.skipif(
    not (_cuda_available() and _gst_cuda_ipc_available()),
    reason="needs cupy, a CUDA device and the GStreamer CUDA IPC elements",
)


@pytest.fixture
def export(tmp_path):
    """A 64x48 solid-red BT.709 NV12 CUDA IPC export, like Oden's."""
    address = str(tmp_path / "oden_test")
    proc = subprocess.Popen(
        [
            "gst-launch-1.0",
            "-q",
            "videotestsrc",
            "pattern=solid-color",
            "foreground-color=0xffff0000",
            "is-live=true",
            "!",
            "video/x-raw,width=64,height=48,framerate=30/1",
            "!",
            "videoconvert",
            "!",
            "video/x-raw,format=NV12,colorimetry=bt709",
            "!",
            "cudaupload",
            "!",
            "cudaipcsink",
            f"address={address}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    yield address
    proc.terminate()
    proc.wait(timeout=5)


def _collect(source, n: int, timeout_s: float = 10.0) -> list:
    frames = []
    deadline = time.monotonic() + timeout_s
    while len(frames) < n and time.monotonic() < deadline:
        f = source.latest()
        if f is not None:
            frames.append(f.image.get())
        else:
            time.sleep(0.005)
    return frames


@requires_export
def test_reads_export(export):
    (source,) = build_local_camera(
        {
            "name": "robot",
            "type": "oden",
            "share_path": export,
            "width": 64,
            "height": 48,
        }
    )
    source.start()
    try:
        frames = _collect(source, 5)
    finally:
        source.stop()
    assert len(frames) == 5
    frame = frames[-1]
    assert frame.shape == (48, 64, 4)
    assert _close(tuple(int(c) for c in frame[24, 32]), (255, 0, 0, 255), 4)


@requires_export
def test_size_mismatch_is_reported(export):
    messages = []
    set_notify_sink(messages.append)
    (source,) = build_local_camera(
        {
            "name": "robot",
            "type": "oden",
            "share_path": export,
            "width": 32,
            "height": 24,
        }
    )
    source.start()
    try:
        frames = _collect(source, 1, timeout_s=4.0)
    finally:
        source.stop()
        set_notify_sink(None)
    assert frames == []
    assert any("export is 64x48" in m for m in messages), messages


@requires_export
def test_waits_for_export_and_says_so_once(tmp_path):
    address = str(tmp_path / "nobody")
    messages = []
    set_notify_sink(messages.append)
    (source,) = build_local_camera(
        {
            "name": "robot",
            "type": "oden",
            "share_path": address,
            "width": 64,
            "height": 48,
        }
    )
    source.start()
    try:
        # Long enough for at least two open attempts.
        frames = _collect(source, 1, timeout_s=7.0)
    finally:
        source.stop()
        set_notify_sink(None)
    assert frames == []
    assert sum(address in m for m in messages) == 1, messages
