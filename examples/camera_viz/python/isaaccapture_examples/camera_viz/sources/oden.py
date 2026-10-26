# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Oden CUDA IPC texture export source for camera_viz.

Reads one CUDA IPC Texture Export of Voysys Oden. The usual setup is an
Oden Player on this workstation that receives the robot's cameras from an
Oden Streamer over Oden's transport and exports each camera here. Oden
shares every frame as NV12 in GPU memory over GStreamer's CUDA IPC
protocol (``cudaipcsink``). This source runs ``cudaipcsrc ! appsink`` in
CuPy's CUDA context and converts each frame from Oden's buffer straight
into the RGBA output buffer with one kernel, so the frame never leaves
the GPU and Oden gets its buffer back as soon as the kernel finishes.

GStreamer is driven through ctypes rather than PyGObject: a buffer mapped
for CUDA access yields a device pointer, which PyGObject's buffer mapping
would read as host memory.

The export must have the camera's ``width`` and ``height`` (set Share
Width / Share Height in Oden); a mismatch fails the connection with the
sizes in the error.
"""

from __future__ import annotations

import ctypes
import logging
import threading
import time
from typing import Optional

from ._helpers import PolledSource, notify

# GST_MAP_FLAG_LAST << 1: map for CUDA access (gstcudamemory.h).
_GST_MAP_READ = 1
_GST_MAP_CUDA = (1 << 16) << 1
_GST_STATE_NULL = 1
_GST_STATE_PLAYING = 4
_GST_STATE_CHANGE_FAILURE = 0
_GST_MESSAGE_EOS = 1 << 0
_GST_MESSAGE_ERROR = 1 << 1

# How long one pull waits for a frame before the producer loop gets the
# chance to notice stop() or a bus error.
_PULL_TIMEOUT_NS = 50_000_000

# cudaipcsrc posts neither an error nor EOS when Oden closes the export (a
# Player restart), it just stops delivering. Reconnect after this long
# without a frame.
_STALL_TIMEOUT_S = 3.0

# How long opening waits for the export's first frame.
_FIRST_FRAME_TIMEOUT_S = 2.0


class _GstMapInfo(ctypes.Structure):
    _fields_ = [
        ("memory", ctypes.c_void_p),
        ("flags", ctypes.c_int),
        ("data", ctypes.c_void_p),
        ("size", ctypes.c_size_t),
        ("maxsize", ctypes.c_size_t),
        ("user_data", ctypes.c_void_p * 4),
        ("_gst_reserved", ctypes.c_void_p * 4),
    ]


class _GstVideoMeta(ctypes.Structure):
    _fields_ = [
        ("meta_flags", ctypes.c_int),
        ("meta_info", ctypes.c_void_p),
        ("buffer", ctypes.c_void_p),
        ("flags", ctypes.c_int),
        ("format", ctypes.c_int),
        ("id", ctypes.c_int),
        ("width", ctypes.c_uint),
        ("height", ctypes.c_uint),
        ("n_planes", ctypes.c_uint),
        ("offset", ctypes.c_size_t * 4),
        ("stride", ctypes.c_int * 4),
    ]


class _GError(ctypes.Structure):
    _fields_ = [
        ("domain", ctypes.c_uint32),
        ("code", ctypes.c_int),
        ("message", ctypes.c_char_p),
    ]


class _Gst:
    """The few GStreamer C functions this source calls, loaded once."""

    _lock = threading.Lock()
    _instance: Optional["_Gst"] = None

    @classmethod
    def get(cls) -> "_Gst":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self) -> None:
        def load(name: str) -> ctypes.CDLL:
            try:
                return ctypes.CDLL(name)
            except OSError as e:
                raise RuntimeError(
                    f"OdenSource needs {name} (GStreamer 1.24 or newer with "
                    f"gstreamer1.0-plugins-bad): {e}"
                ) from e

        gst = load("libgstreamer-1.0.so.0")
        app = load("libgstapp-1.0.so.0")
        video = load("libgstvideo-1.0.so.0")
        cuda = load("libgstcuda-1.0.so.0")
        glib = load("libglib-2.0.so.0")
        vp, u64, i32, c_char_p = (
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_int,
            ctypes.c_char_p,
        )

        def fn(lib, name, restype, *argtypes):
            f = getattr(lib, name)
            f.restype = restype
            f.argtypes = list(argtypes)
            return f

        self.init = fn(gst, "gst_init", None, vp, vp)
        self.parse_launch = fn(
            gst, "gst_parse_launch", vp, c_char_p, ctypes.POINTER(vp)
        )
        self.bin_get_by_name = fn(gst, "gst_bin_get_by_name", vp, vp, c_char_p)
        self.element_set_state = fn(gst, "gst_element_set_state", i32, vp, i32)
        self.element_set_context = fn(gst, "gst_element_set_context", None, vp, vp)
        self.element_get_bus = fn(gst, "gst_element_get_bus", vp, vp)
        self.bus_pop_filtered = fn(gst, "gst_bus_pop_filtered", vp, vp, i32)
        self.message_parse_error = fn(
            gst,
            "gst_message_parse_error",
            None,
            vp,
            ctypes.POINTER(vp),
            ctypes.POINTER(vp),
        )
        self.error_free = fn(glib, "g_error_free", None, vp)
        self.g_free = fn(glib, "g_free", None, vp)
        self.object_unref = fn(gst, "gst_object_unref", None, vp)
        self.mini_object_unref = fn(gst, "gst_mini_object_unref", None, vp)
        self.sample_get_buffer = fn(gst, "gst_sample_get_buffer", vp, vp)
        self.sample_get_caps = fn(gst, "gst_sample_get_caps", vp, vp)
        self.caps_to_string = fn(gst, "gst_caps_to_string", vp, vp)
        self.buffer_map = fn(
            gst, "gst_buffer_map", ctypes.c_bool, vp, ctypes.POINTER(_GstMapInfo), i32
        )
        self.buffer_unmap = fn(
            gst, "gst_buffer_unmap", None, vp, ctypes.POINTER(_GstMapInfo)
        )
        self.buffer_get_meta = fn(gst, "gst_buffer_get_meta", vp, vp, u64)
        self.app_sink_try_pull_sample = fn(
            app, "gst_app_sink_try_pull_sample", vp, vp, u64
        )
        self.video_meta_api_get_type = fn(video, "gst_video_meta_api_get_type", u64)
        self.cuda_context_new_wrapped = fn(
            cuda, "gst_cuda_context_new_wrapped", vp, vp, i32
        )
        self.context_new_cuda_context = fn(cuda, "gst_context_new_cuda_context", vp, vp)
        self.is_cuda_memory = fn(cuda, "gst_is_cuda_memory", ctypes.c_bool, vp)
        self.cuda_memory_sync = fn(cuda, "gst_cuda_memory_sync", None, vp)
        cuda_load_library = fn(cuda, "gst_cuda_load_library", ctypes.c_bool)

        self.init(None, None)
        # libgstcuda resolves the CUDA driver lazily, normally when the nvcodec
        # plugin loads; this source calls it before any CUDA element exists.
        if not cuda_load_library():
            raise RuntimeError("OdenSource: GStreamer could not load the CUDA driver")
        self.video_meta_type = self.video_meta_api_get_type()

    def take_string(self, ptr) -> str:
        if not ptr:
            return ""
        text = ctypes.cast(ptr, ctypes.c_char_p).value.decode(errors="replace")
        self.g_free(ptr)
        return text


_NV12_TO_RGBA_KERNEL_SRC = r"""
extern "C" __global__ void oden_nv12_to_rgba(
    const unsigned char* __restrict__ y_plane,
    int y_pitch,
    const unsigned char* __restrict__ uv_plane,
    int uv_pitch,
    int width,
    int height,
    float y_offset,
    float y_scale,
    float cr_r,
    float cb_g,
    float cr_g,
    float cb_b,
    unsigned char* __restrict__ rgba,
    int rgba_row_bytes)
{
    int x = blockIdx.x * blockDim.x + threadIdx.x;
    int y = blockIdx.y * blockDim.y + threadIdx.y;
    if (x >= width || y >= height) return;

    float Y = ((float)y_plane[y * y_pitch + x] - y_offset) * y_scale;
    int uv_idx = (y >> 1) * uv_pitch + (x & ~1);
    float Cb = (float)uv_plane[uv_idx + 0] - 128.f;
    float Cr = (float)uv_plane[uv_idx + 1] - 128.f;

    float R = Y + cr_r * Cr;
    float G = Y + cb_g * Cb + cr_g * Cr;
    float B = Y + cb_b * Cb;

    int o = y * rgba_row_bytes + x * 4;
    rgba[o + 0] = (unsigned char)fminf(fmaxf(R + 0.5f, 0.f), 255.f);
    rgba[o + 1] = (unsigned char)fminf(fmaxf(G + 0.5f, 0.f), 255.f);
    rgba[o + 2] = (unsigned char)fminf(fmaxf(B + 0.5f, 0.f), 255.f);
}
"""


def _ycbcr_coefficients(colorimetry: str) -> tuple:
    """(y_offset, y_scale, cr_r, cb_g, cr_g, cb_b) for a caps colorimetry.

    Oden exports BT.709 limited range ("2:3:7:1" or "bt709"); BT.601 and
    full range are handled too. Anything else is read as BT.709 limited.
    """
    parts = colorimetry.split(":")
    if colorimetry in ("bt601", "smpte170m", "jpeg") or (
        len(parts) == 4 and parts[1] == "4"
    ):
        kr, kb = 0.299, 0.114
    else:
        kr, kb = 0.2126, 0.0722
    full_range = colorimetry == "jpeg" or (len(parts) == 4 and parts[0] == "1")
    kg = 1.0 - kr - kb
    if full_range:
        y_offset, y_scale, c_scale = 0.0, 1.0, 1.0
    else:
        y_offset, y_scale, c_scale = 16.0, 255.0 / 219.0, 255.0 / 224.0
    cr_r = 2.0 * (1.0 - kr) * c_scale
    cb_b = 2.0 * (1.0 - kb) * c_scale
    cb_g = -2.0 * (1.0 - kb) * kb / kg * c_scale
    cr_g = -2.0 * (1.0 - kr) * kr / kg * c_scale
    return y_offset, y_scale, cr_r, cb_g, cr_g, cb_b


def _caps_field(caps: str, key: str) -> str:
    """Value of ``key`` in a caps string, without its ``(type)`` prefix."""
    for field in caps.split(","):
        name, _, value = field.strip().partition("=")
        if name == key:
            return value.split(")", 1)[-1].strip().strip('"')
    return ""


class OdenSource(PolledSource):
    """One Oden CUDA IPC texture export (NV12 in GPU memory)."""

    _kind = "oden"

    def __init__(
        self,
        name: str,
        share_path: str = "/tmp/oden_0",
        width: int = 1280,
        height: int = 720,
    ) -> None:
        # No host staging: frames go from Oden's GPU buffer to the RGBA
        # output buffer directly, so the base class's staging stays 1 channel.
        super().__init__(name=name, width=width, height=height, staging_channels=1)
        cp = self._cp
        self._share_path = share_path
        self._gst = _Gst.get()
        self._kernel = cp.RawKernel(_NV12_TO_RGBA_KERNEL_SRC, "oden_nv12_to_rgba")

        # cudaipcsrc opens Oden's buffers in whatever CUDA context it is
        # given. Hand it CuPy's (the device's primary context, current here
        # because the base class just allocated on it), so the kernel can
        # read them.
        self._device_id = int(self._gpu_buffers[0].device.id)
        with cp.cuda.Device(self._device_id):
            cu_ctx = cp.cuda.driver.ctxGetCurrent()
        if not cu_ctx:
            raise RuntimeError(f"OdenSource '{name}': no current CUDA context")
        self._gst_cuda_ctx = self._gst.cuda_context_new_wrapped(
            ctypes.c_void_p(cu_ctx), self._device_id
        )
        if not self._gst_cuda_ctx:
            raise RuntimeError(
                f"OdenSource '{name}': could not wrap the CUDA context for GStreamer"
            )

        self._pipeline = None
        self._appsink = None
        self._bus = None
        self._sample = None
        self._first_sample = None
        self._buffer = None
        self._map = _GstMapInfo()
        self._mapped = False
        self._caps_ptr = None
        self._open_error = ""
        self._last_sample_s = 0.0
        self._coefficients = _ycbcr_coefficients("")

    # ── PolledSource hooks ────────────────────────────────────────────

    def _open_device(self) -> bool:
        gst = self._gst
        address = self._share_path.replace('"', '\\"')
        pipeline_str = (
            f'cudaipcsrc name=src address="{address}" ! '
            "video/x-raw(memory:CUDAMemory),format=NV12 ! "
            "appsink name=sink sync=false max-buffers=1 drop=true "
            "emit-signals=false wait-on-eos=false"
        )
        err = ctypes.c_void_p()
        pipeline = gst.parse_launch(pipeline_str.encode(), ctypes.byref(err))
        reason = ""
        if err.value:
            reason = _GError.from_address(err.value).message.decode(errors="replace")
            gst.error_free(err)
        if not pipeline:
            raise RuntimeError(f"cannot build pipeline ({reason}): {pipeline_str}")
        self._pipeline = pipeline
        self._appsink = gst.bin_get_by_name(pipeline, b"sink")
        self._bus = gst.element_get_bus(pipeline)

        gst_context = gst.context_new_cuda_context(self._gst_cuda_ctx)
        gst.element_set_context(pipeline, gst_context)
        gst.mini_object_unref(gst_context)

        message = ""
        if (
            gst.element_set_state(pipeline, _GST_STATE_PLAYING)
            == _GST_STATE_CHANGE_FAILURE
        ):
            message = self._pop_bus_error() or "pipeline failed to start"
        else:
            # cudaipcsrc starts even when nothing is exporting, so only the
            # first frame says Oden is there.
            deadline = time.monotonic() + _FIRST_FRAME_TIMEOUT_S
            while not self._first_sample and time.monotonic() < deadline:
                message = self._pop_bus_error()
                if message:
                    break
                self._first_sample = gst.app_sink_try_pull_sample(
                    self._appsink, _PULL_TIMEOUT_NS
                )
            if not self._first_sample and not message:
                message = "no frames; is an Oden Texture Export sharing this path?"
        if message:
            self._close_device()
            # Retried every couple of seconds while Oden is not exporting:
            # say it once, not on every retry.
            if message != self._open_error:
                notify(self._kind, f"{self._share_path}: {message}", logging.WARNING)
                self._open_error = message
            return False
        self._open_error = ""
        self._caps_ptr = None
        self._last_sample_s = time.monotonic()
        return True

    def _close_device(self) -> None:
        gst = self._gst
        self._release_sample()
        if self._pipeline is not None:
            gst.element_set_state(self._pipeline, _GST_STATE_NULL)
        for ptr in (self._appsink, self._bus, self._pipeline):
            if ptr:
                gst.object_unref(ptr)
        self._pipeline = self._appsink = self._bus = None

    def _grab(self):
        gst = self._gst
        message = self._pop_bus_error()
        if message:
            raise RuntimeError(message)
        sample, self._first_sample = self._first_sample, None
        if not sample:
            sample = gst.app_sink_try_pull_sample(self._appsink, _PULL_TIMEOUT_NS)
        if not sample:
            if time.monotonic() - self._last_sample_s > _STALL_TIMEOUT_S:
                raise RuntimeError(f"no frames for {_STALL_TIMEOUT_S:.0f} s")
            return None
        self._sample = sample
        self._last_sample_s = time.monotonic()

        caps = gst.sample_get_caps(sample)
        if caps != self._caps_ptr:
            self._check_caps(gst.take_string(gst.caps_to_string(caps)))
            self._caps_ptr = caps

        buffer = gst.sample_get_buffer(sample)
        if not gst.buffer_map(
            buffer, ctypes.byref(self._map), _GST_MAP_READ | _GST_MAP_CUDA
        ):
            self._release_sample()
            raise RuntimeError("cannot map the export buffer for CUDA")
        self._buffer = buffer
        self._mapped = True
        if not gst.is_cuda_memory(self._map.memory):
            raise RuntimeError("the export buffer is not CUDA memory")
        # Oden's writes may still be in flight on its stream.
        gst.cuda_memory_sync(self._map.memory)
        return self._map

    def _upload_and_convert(self, gpu_buf) -> None:
        cp = self._cp
        try:
            width, height = self._spec.width, self._spec.height
            y_offset, y_pitch, uv_offset, uv_pitch = self._plane_layout()
            nv12 = cp.ndarray(
                (self._map.size,),
                dtype=cp.uint8,
                memptr=cp.cuda.MemoryPointer(
                    cp.cuda.UnownedMemory(
                        self._map.data, self._map.size, self, self._device_id
                    ),
                    0,
                ),
            )
            block = (16, 16, 1)
            grid = ((width + 15) // 16, (height + 15) // 16, 1)
            with self._stream:
                self._kernel(
                    grid,
                    block,
                    (
                        nv12[y_offset:],
                        cp.int32(y_pitch),
                        nv12[uv_offset:],
                        cp.int32(uv_pitch),
                        cp.int32(width),
                        cp.int32(height),
                        *(cp.float32(c) for c in self._coefficients),
                        gpu_buf,
                        cp.int32(gpu_buf.strides[0]),
                    ),
                )
            self._stream.synchronize()
        finally:
            self._release_sample()

    # ── Internals ─────────────────────────────────────────────────────

    def _check_caps(self, caps: str) -> None:
        width = _caps_field(caps, "width")
        height = _caps_field(caps, "height")
        if (width, height) != (str(self._spec.width), str(self._spec.height)):
            raise ValueError(
                f"export is {width}x{height}, camera '{self._spec.name}' is "
                f"{self._spec.width}x{self._spec.height}; set Share Width / "
                "Share Height in Oden to the camera's size"
            )
        colorimetry = _caps_field(caps, "colorimetry")
        self._coefficients = _ycbcr_coefficients(colorimetry)
        notify(
            self._kind,
            f"{self._share_path}: {width}x{height} NV12, colorimetry {colorimetry or 'unset'}",
        )

    def _plane_layout(self) -> tuple:
        """(Y offset, Y pitch, UV offset, UV pitch) of the mapped NV12 frame."""
        meta_ptr = self._gst.buffer_get_meta(self._buffer, self._gst.video_meta_type)
        if meta_ptr:
            meta = _GstVideoMeta.from_address(meta_ptr)
            return meta.offset[0], meta.stride[0], meta.offset[1], meta.stride[1]
        width, height = self._spec.width, self._spec.height
        return 0, width, width * height, width

    def _pop_bus_error(self) -> str:
        gst = self._gst
        if not self._bus:
            return ""
        msg = gst.bus_pop_filtered(self._bus, _GST_MESSAGE_ERROR)
        if msg:
            err = ctypes.c_void_p()
            debug = ctypes.c_void_p()
            gst.message_parse_error(msg, ctypes.byref(err), ctypes.byref(debug))
            text = "pipeline error"
            if err.value:
                text = _GError.from_address(err.value).message.decode(errors="replace")
                gst.error_free(err)
            gst.g_free(debug)
            gst.mini_object_unref(msg)
            return text
        msg = gst.bus_pop_filtered(self._bus, _GST_MESSAGE_EOS)
        if msg:
            gst.mini_object_unref(msg)
            return "the export ended"
        return ""

    def _release_sample(self) -> None:
        gst = self._gst
        if self._first_sample:
            gst.mini_object_unref(self._first_sample)
            self._first_sample = None
        if self._mapped:
            gst.buffer_unmap(self._buffer, ctypes.byref(self._map))
            self._mapped = False
        self._buffer = None
        if self._sample:
            gst.mini_object_unref(self._sample)
            self._sample = None
