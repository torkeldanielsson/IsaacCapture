# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Split ROS 2 messages into a schema and packed values, and rebuild them.

Strings and the lengths of string or message sequences make up the schema, which
rarely changes and crosses the link only until the receiver has it. Numbers and
the lengths of number sequences are packed little-endian into the values sent
with every sample. Every ``builtin_interfaces/Time`` (and every msgpack time key)
goes through ``time_map`` when rebuilt, so the receiver can move stamps into its
own clock; zero stamps stay zero.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import struct
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable

from rosidl_parser.definition import (
    AbstractGenericString,
    AbstractSequence,
    Array,
    BasicType,
    NamespacedType,
)

from .config import TopicSpec

TimeMap = Callable[[int], int]

_NS = 1_000_000_000

_CODES = {
    "boolean": "?",
    "octet": "B",
    "char": "B",
    "wchar": "H",
    "int8": "b",
    "uint8": "B",
    "int16": "h",
    "short": "h",
    "uint16": "H",
    "unsigned short": "H",
    "int32": "i",
    "long": "i",
    "uint32": "I",
    "unsigned long": "I",
    "int64": "q",
    "long long": "q",
    "uint64": "Q",
    "unsigned long long": "Q",
    "float": "f",
    "double": "d",
    "long double": "d",
}
_DOUBLES = {"double", "long double"}


@dataclass(frozen=True)
class Packed:
    schema: dict
    schema_id: str
    values: bytes
    # Newest non-zero sender-clock stamp in the message, if it has any.
    stamp_ns: int | None


@lru_cache(maxsize=None)
def message_class(type_name: str):
    """``geometry_msgs/msg/Pose`` -> the generated Python class."""
    package, kind, name = type_name.split("/")
    return getattr(importlib.import_module(f"{package}.{kind}"), name)


def _slot_class(slot: NamespacedType):
    return message_class("/".join((*slot.namespaces, slot.name)))


def _is_time(slot: NamespacedType) -> bool:
    return slot.name == "Time" and tuple(slot.namespaces) == (
        "builtin_interfaces",
        "msg",
    )


@lru_cache(maxsize=None)
def _fields(cls) -> tuple[tuple[str, Any], ...]:
    return tuple(zip(cls.get_fields_and_field_types(), cls.SLOT_TYPES))


def schema_id(schema: dict) -> str:
    text = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode()).hexdigest()[:10]


class _Writer:
    def __init__(self, float32: bool):
        self.tokens: list = []
        self.fmt: list[str] = ["<"]
        self.values: list = []
        self.stamp_ns: int | None = None
        self._double = "f" if float32 else "d"

    def code(self, slot: BasicType) -> str:
        return self._double if slot.typename in _DOUBLES else _CODES[slot.typename]

    def number(self, code: str, value) -> None:
        self.fmt.append(code)
        self.values.append(value)

    def stamp(self, ns: int) -> None:
        self.number("q", ns)
        if ns and (self.stamp_ns is None or ns > self.stamp_ns):
            self.stamp_ns = ns

    def pack(self) -> bytes:
        return struct.pack("".join(self.fmt), *self.values)


def _scalar_out(slot: BasicType, value):
    if slot.typename == "octet":
        return value[0]
    if slot.typename in ("char", "wchar") and isinstance(value, str):
        return ord(value)
    if slot.typename == "boolean":
        return bool(value)
    return value


def _scalar_in(slot: BasicType, value):
    if slot.typename == "octet":
        return bytes((value,))
    if slot.typename in ("char", "wchar"):
        return chr(value)
    return value


def _encode_message(msg, w: _Writer) -> None:
    for name, slot in _fields(type(msg)):
        _encode_value(slot, getattr(msg, name), w)


def _encode_value(slot, value, w: _Writer) -> None:
    if isinstance(slot, BasicType):
        w.number(w.code(slot), _scalar_out(slot, value))
    elif isinstance(slot, AbstractGenericString):
        w.tokens.append(value)
    elif isinstance(slot, NamespacedType):
        if _is_time(slot):
            w.stamp(value.sec * _NS + value.nanosec)
        else:
            _encode_message(value, w)
    elif isinstance(slot, (Array, AbstractSequence)):
        items = value
        element = slot.value_type
        fixed = isinstance(slot, Array)
        if isinstance(element, BasicType):
            if not fixed:
                w.number("I", len(items))
            if element.typename == "octet":
                w.fmt.append(f"{len(items)}s")
                w.values.append(b"".join(items))
            else:
                w.fmt.append(f"{len(items)}{w.code(element)}")
                w.values.extend(_scalar_out(element, v) for v in items)
        else:
            if not fixed:
                w.tokens.append(len(items))
            for item in items:
                _encode_value(element, item, w)
    else:
        raise TypeError(f"unsupported ROS field type {slot!r}")


class _Reader:
    def __init__(self, tokens: list, values: bytes, time_map: TimeMap):
        self._tokens = iter(tokens)
        self._values = values
        self._offset = 0
        self._time_map = time_map

    def token(self):
        return next(self._tokens)

    def numbers(self, fmt: str) -> tuple:
        out = struct.unpack_from("<" + fmt, self._values, self._offset)
        self._offset += struct.calcsize("<" + fmt)
        return out

    def stamp(self) -> int:
        (ns,) = self.numbers("q")
        return self._time_map(ns) if ns else 0

    def finish(self) -> None:
        if self._offset != len(self._values) or next(self._tokens, None) is not None:
            raise ValueError("schema and values disagree")


def _decode_message(cls, r: _Reader, float32: bool):
    msg = cls()
    for name, slot in _fields(cls):
        setattr(msg, name, _decode_value(slot, r, float32))
    return msg


def _decode_value(slot, r: _Reader, float32: bool):
    def code(basic: BasicType) -> str:
        return "f" if float32 and basic.typename in _DOUBLES else _CODES[basic.typename]

    if isinstance(slot, BasicType):
        return _scalar_in(slot, r.numbers(code(slot))[0])
    if isinstance(slot, AbstractGenericString):
        return r.token()
    if isinstance(slot, NamespacedType):
        if _is_time(slot):
            ns = r.stamp()
            time = _slot_class(slot)()
            time.sec, time.nanosec = divmod(ns, _NS)
            return time
        return _decode_message(_slot_class(slot), r, float32)
    if isinstance(slot, (Array, AbstractSequence)):
        element = slot.value_type
        fixed = isinstance(slot, Array)
        if isinstance(element, BasicType):
            count = slot.size if fixed else r.numbers("I")[0]
            if element.typename == "octet":
                (raw,) = r.numbers(f"{count}s")
                return [bytes((b,)) for b in raw]
            return [
                _scalar_in(element, v) for v in r.numbers(f"{count}{code(element)}")
            ]
        count = slot.size if fixed else r.token()
        return [_decode_value(element, r, float32) for _ in range(count)]
    raise TypeError(f"unsupported ROS field type {slot!r}")


# msgpack payloads: every node writes a type token, strings and map keys go to the
# schema, numbers to the values.


def _encode_msgpack(obj, w: _Writer, time_keys: tuple[str, ...], key=None) -> None:
    if obj is None:
        w.tokens.append("~")
    elif isinstance(obj, bool):
        w.tokens.append("?")
        w.number("?", obj)
    elif isinstance(obj, int):
        if key in time_keys:
            w.tokens.append("t")
            w.stamp(obj)
        elif -(2**63) <= obj < 2**63:
            w.tokens.append("i")
            w.number("q", obj)
        else:
            w.tokens.append("u")
            w.number("Q", obj)
    elif isinstance(obj, float):
        w.tokens.append("f")
        w.number(w._double, obj)
    elif isinstance(obj, str):
        w.tokens += ["s", obj]
    elif isinstance(obj, (bytes, bytearray)):
        w.tokens.append("y")
        w.number("I", len(obj))
        w.fmt.append(f"{len(obj)}s")
        w.values.append(bytes(obj))
    elif isinstance(obj, (list, tuple)):
        # Plain float and bool lists (poses, flags) cost one schema token, not one per item.
        if obj and all(type(v) is float for v in obj):
            w.tokens += ["F", len(obj)]
            w.fmt.append(f"{len(obj)}{w._double}")
            w.values.extend(obj)
        elif obj and all(type(v) is bool for v in obj):
            w.tokens += ["B", len(obj)]
            w.fmt.append(f"{len(obj)}?")
            w.values.extend(obj)
        else:
            w.tokens += ["[", len(obj)]
            for item in obj:
                _encode_msgpack(item, w, time_keys)
    elif isinstance(obj, dict):
        if not all(isinstance(k, str) for k in obj):
            raise TypeError("msgpack map keys must be strings")
        w.tokens += ["{", len(obj)]
        for k, v in obj.items():
            w.tokens.append(k)
            _encode_msgpack(v, w, time_keys, k)
    else:
        raise TypeError(f"unsupported msgpack value {type(obj).__name__}")


def _decode_msgpack(r: _Reader, float32: bool):
    kind = r.token()
    if kind == "~":
        return None
    if kind == "?":
        return r.numbers("?")[0]
    if kind == "t":
        return r.stamp()
    if kind == "i":
        return r.numbers("q")[0]
    if kind == "u":
        return r.numbers("Q")[0]
    if kind == "f":
        return r.numbers("f" if float32 else "d")[0]
    if kind == "s":
        return r.token()
    if kind == "y":
        (count,) = r.numbers("I")
        return r.numbers(f"{count}s")[0]
    if kind == "F":
        return list(r.numbers(f"{r.token()}{'f' if float32 else 'd'}"))
    if kind == "B":
        return list(r.numbers(f"{r.token()}?"))
    if kind == "[":
        return [_decode_msgpack(r, float32) for _ in range(r.token())]
    if kind == "{":
        out = {}
        for _ in range(r.token()):
            k = r.token()
            out[k] = _decode_msgpack(r, float32)
        return out
    raise ValueError(f"bad msgpack schema token {kind!r}")


def _pack_msgpack(spec: TopicSpec, msg, float32: bool) -> Packed | None:
    import msgpack

    try:
        payload = msgpack.unpackb(b"".join(msg.data), raw=False)
        w = _Writer(float32)
        _encode_msgpack(payload, w, spec.time_keys)
    except (TypeError, ValueError, msgpack.UnpackException):
        return None
    schema = {
        "topic": spec.topic,
        "type": spec.type,
        "f32": float32,
        "msgpack": w.tokens,
    }
    return Packed(schema, schema_id(schema), w.pack(), w.stamp_ns)


def _pack_ros(spec: TopicSpec, msg, float32: bool) -> Packed:
    w = _Writer(float32)
    _encode_message(msg, w)
    schema = {
        "topic": spec.topic,
        "type": spec.type,
        "f32": float32,
        "tokens": w.tokens,
    }
    return Packed(schema, schema_id(schema), w.pack(), w.stamp_ns)


def _pack(spec: TopicSpec, msg, float32: bool) -> Packed:
    if spec.payload == "msgpack":
        packed = _pack_msgpack(spec, msg, float32)
        if packed is not None:
            return packed
    return _pack_ros(spec, msg, float32)


def pack(spec: TopicSpec, msg) -> Packed:
    """Flatten ``msg`` of ``spec.type``.

    A msgpack topic whose payload does not decode goes as a plain message, and a
    float32 topic holding a value float32 cannot represent goes as float64.
    """
    if spec.float32:
        try:
            return _pack(spec, msg, True)
        except (OverflowError, struct.error):
            pass
    return _pack(spec, msg, False)


def unpack(schema: dict, values: bytes, time_map: TimeMap):
    """Rebuild the message ``pack`` flattened, with every stamp passed through ``time_map``."""
    cls = message_class(schema["type"])
    float32 = bool(schema.get("f32"))
    if "msgpack" in schema:
        import msgpack

        r = _Reader(schema["msgpack"], values, time_map)
        payload = _decode_msgpack(r, float32)
        r.finish()
        msg = cls()
        msg.data = [bytes((b,)) for b in msgpack.packb(payload)]
        return msg
    r = _Reader(schema["tokens"], values, time_map)
    msg = _decode_message(cls, r, float32)
    r.finish()
    return msg
