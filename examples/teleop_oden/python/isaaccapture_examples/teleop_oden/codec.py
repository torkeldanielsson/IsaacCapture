# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""Split samples into a schema and a flat list of values, and rebuild them.

The schema holds what rarely changes: the topic and type, strings, map keys and
the length of every sequence. It crosses the link only until the receiver has it.
The values are the numbers in field order: integers and booleans as integers,
floats as whole multiples of the channel's ``precision`` when it has one and as
floats otherwise, byte arrays as base64 text. A sample with a float the precision
cannot carry (NaN, infinity, huge values) goes with plain floats under its own
schema. ROS 2 messages of any type, msgpack payloads in a ByteMultiArray, and ZMQ
payloads (raw bytes, or JSON) are carried this way.
"""

from __future__ import annotations

import base64
import importlib
import json
import math
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

try:
    from rosidl_parser.definition import (
        AbstractGenericString,
        AbstractSequence,
        Array,
        BasicType,
        NamespacedType,
    )
except ImportError:  # ZMQ streams need no ROS
    pass

# What OCP's JSON numbers hold.
_INT_MIN, _INT_END = -(2**63), 2**64
_FLOATS = {"float", "double", "long double"}
_BYTE_ARRAYS = {"octet", "uint8"}


@dataclass(frozen=True)
class Flat:
    schema: dict
    values: list


class _NeedsFloats(Exception):
    """A float the precision cannot carry."""


@lru_cache(maxsize=None)
def message_class(type_name: str):
    """``geometry_msgs/msg/Pose`` -> the generated Python class."""
    package, kind, name = type_name.split("/")
    return getattr(importlib.import_module(f"{package}.{kind}"), name)


def channel_key(schema: dict) -> str:
    """The channel a schema belongs to: ``t:<topic>`` or ``z:<stream>``."""
    return f"t:{schema['t']}" if "t" in schema else f"z:{schema['z']}"


class _Scale:
    """Floats to whole multiples of ``precision`` and back."""

    def __init__(self, precision: float):
        self.precision = precision
        inverse = 1.0 / precision
        # 0.0001 -> divide by 10000.0, which rounds better than multiplying.
        self._inverse = (
            round(inverse) if abs(inverse - round(inverse)) < 1e-9 * inverse else None
        )

    def encode(self, value: float) -> int:
        q = value * self._inverse if self._inverse else value / self.precision
        if not math.isfinite(q) or abs(q) >= 2**53:
            raise _NeedsFloats
        return round(q)

    def decode(self, q: int) -> float:
        return q / self._inverse if self._inverse else q * self.precision


class _Out:
    def __init__(self, precision: float | None):
        self.tokens: list = []
        self.values: list = []
        self._scale = _Scale(precision) if precision else None

    def float(self, value) -> None:
        value = float(value)
        if self._scale is not None:
            self.values.append(self._scale.encode(value))
        elif math.isfinite(value):
            self.values.append(value)
        else:
            # JSON has no NaN or infinity.
            self.values.append(repr(value))

    def int(self, value: int) -> None:
        if not _INT_MIN <= value < _INT_END:
            raise TypeError(f"integer {value} does not fit 64 bits")
        self.values.append(value)

    def bytes(self, raw: bytes) -> None:
        self.values.append(base64.b64encode(raw).decode())


class _In:
    def __init__(self, tokens: list, values: list, precision: float | None):
        self._tokens = iter(tokens)
        self._values = iter(values)
        self._scale = _Scale(precision) if precision else None

    def token(self):
        item = next(self._tokens, _END)
        if item is _END:
            raise ValueError("schema ends early")
        return item

    def value(self):
        item = next(self._values, _END)
        if item is _END:
            raise ValueError("values end early")
        return item

    def float(self) -> float:
        v = self.value()
        if isinstance(v, str):
            return float(v)
        if self._scale is not None and isinstance(v, int):
            return self._scale.decode(v)
        return float(v)

    def int(self) -> int:
        v = self.value()
        if not isinstance(v, int):
            raise ValueError(f"expected an integer, got {v!r}")
        return v

    def bytes(self) -> bytes:
        return base64.b64decode(self.value())

    def finish(self) -> None:
        if next(self._tokens, _END) is not _END or next(self._values, _END) is not _END:
            raise ValueError("schema and values disagree")


_END = object()


# ROS 2 messages, walked through their rosidl type description.


@lru_cache(maxsize=None)
def _fields(cls) -> tuple[tuple[str, Any], ...]:
    return tuple(zip(cls.get_fields_and_field_types(), cls.SLOT_TYPES))


def _slot_class(slot):
    return message_class("/".join((*slot.namespaces, slot.name)))


def _basic_out(typename: str, value, out: _Out) -> None:
    if typename in _FLOATS:
        out.float(value)
    elif typename == "boolean":
        out.int(1 if value else 0)
    elif typename == "octet":
        out.int(value[0])
    elif typename in ("char", "wchar"):
        out.int(ord(value) if isinstance(value, str) else int(value))
    else:
        out.int(int(value))


def _basic_in(typename: str, r: _In):
    if typename in _FLOATS:
        return r.float()
    v = r.int()
    if typename == "boolean":
        return bool(v)
    if typename == "octet":
        return bytes((v,))
    if typename in ("char", "wchar"):
        return chr(v)
    return v


def _message_out(msg, out: _Out) -> None:
    for name, slot in _fields(type(msg)):
        _value_out(slot, getattr(msg, name), out)


def _value_out(slot, value, out: _Out) -> None:
    if isinstance(slot, BasicType):
        _basic_out(slot.typename, value, out)
    elif isinstance(slot, AbstractGenericString):
        out.tokens.append(value)
    elif isinstance(slot, NamespacedType):
        _message_out(value, out)
    elif isinstance(slot, (Array, AbstractSequence)):
        element = slot.value_type
        if isinstance(element, BasicType) and element.typename in _BYTE_ARRAYS:
            out.bytes(b"".join(value) if element.typename == "octet" else bytes(value))
            return
        if not isinstance(slot, Array):
            out.tokens.append(len(value))
        for item in value:
            _value_out(element, item, out)
    else:
        raise TypeError(f"unsupported ROS field type {slot!r}")


def _message_in(cls, r: _In):
    msg = cls()
    for name, slot in _fields(cls):
        setattr(msg, name, _value_in(slot, r))
    return msg


def _value_in(slot, r: _In):
    if isinstance(slot, BasicType):
        return _basic_in(slot.typename, r)
    if isinstance(slot, AbstractGenericString):
        return r.token()
    if isinstance(slot, NamespacedType):
        return _message_in(_slot_class(slot), r)
    if isinstance(slot, (Array, AbstractSequence)):
        element = slot.value_type
        if isinstance(element, BasicType) and element.typename in _BYTE_ARRAYS:
            raw = r.bytes()
            return (
                [bytes((b,)) for b in raw] if element.typename == "octet" else list(raw)
            )
        count = slot.size if isinstance(slot, Array) else r.token()
        return [_value_in(element, r) for _ in range(count)]
    raise TypeError(f"unsupported ROS field type {slot!r}")


# Trees: decoded msgpack or JSON. Every node writes a kind token; strings and map
# keys go to the schema, numbers to the values.


def _tree_out(obj, out: _Out) -> None:
    if obj is None:
        out.tokens.append("~")
    elif isinstance(obj, bool):
        out.tokens.append("?")
        out.int(int(obj))
    elif isinstance(obj, int):
        out.tokens.append("i")
        out.int(obj)
    elif isinstance(obj, float):
        out.tokens.append("f")
        out.float(obj)
    elif isinstance(obj, str):
        out.tokens += ["s", obj]
    elif isinstance(obj, (bytes, bytearray)):
        out.tokens.append("y")
        out.bytes(bytes(obj))
    elif isinstance(obj, (list, tuple)):
        # Plain float, bool and int lists (poses, flags) cost one schema token.
        kinds = {type(v) for v in obj}
        if obj and kinds == {float}:
            out.tokens += ["F", len(obj)]
            for v in obj:
                out.float(v)
        elif obj and kinds == {bool}:
            out.tokens += ["B", len(obj)]
            for v in obj:
                out.int(int(v))
        elif obj and kinds == {int}:
            out.tokens += ["I", len(obj)]
            for v in obj:
                out.int(v)
        else:
            out.tokens += ["[", len(obj)]
            for item in obj:
                _tree_out(item, out)
    elif isinstance(obj, dict):
        if not all(isinstance(k, str) for k in obj):
            raise TypeError("map keys must be strings")
        out.tokens += ["{", len(obj)]
        for k, v in obj.items():
            out.tokens.append(k)
            _tree_out(v, out)
    else:
        raise TypeError(f"unsupported value {type(obj).__name__}")


def _tree_in(r: _In):
    kind = r.token()
    if kind == "~":
        return None
    if kind == "?":
        return bool(r.int())
    if kind == "i":
        return r.int()
    if kind == "f":
        return r.float()
    if kind == "s":
        return r.token()
    if kind == "y":
        return r.bytes()
    if kind == "F":
        return [r.float() for _ in range(r.token())]
    if kind == "B":
        return [bool(r.int()) for _ in range(r.token())]
    if kind == "I":
        return [r.int() for _ in range(r.token())]
    if kind == "[":
        return [_tree_in(r) for _ in range(r.token())]
    if kind == "{":
        out = {}
        for _ in range(r.token()):
            k = r.token()
            out[k] = _tree_in(r)
        return out
    raise ValueError(f"bad schema token {kind!r}")


def _with_precision(schema: dict, precision: float | None, build) -> Flat:
    """``build(precision) -> (tokens_key, tokens, values)``, falling back to plain floats."""
    if precision:
        try:
            key, tokens, values = build(precision)
            return Flat({**schema, key: tokens, "p": precision}, values)
        except _NeedsFloats:
            pass
    key, tokens, values = build(None)
    return Flat({**schema, key: tokens}, values)


def flatten_topic(
    topic: str,
    type_name: str,
    msg,
    precision: float | None = None,
    msgpack_payload: bool = False,
) -> Flat:
    """Flatten ``msg``; a msgpack payload that does not decode goes as the plain message."""
    schema = {"t": topic, "y": type_name}
    if msgpack_payload:
        import msgpack

        try:
            payload = msgpack.unpackb(b"".join(msg.data), raw=False)

            def build_tree(p):
                out = _Out(p)
                _tree_out(payload, out)
                return "m", out.tokens, out.values

            return _with_precision(schema, precision, build_tree)
        except (TypeError, ValueError, msgpack.UnpackException):
            pass

    def build_message(p):
        out = _Out(p)
        _message_out(msg, out)
        return "k", out.tokens, out.values

    return _with_precision(schema, precision, build_message)


def rebuild_topic(schema: dict, values: list):
    cls = message_class(schema["y"])
    precision = schema.get("p")
    if "m" in schema:
        import msgpack

        r = _In(schema["m"], values, precision)
        payload = _tree_in(r)
        r.finish()
        msg = cls()
        msg.data = [bytes((b,)) for b in msgpack.packb(payload)]
        return msg
    r = _In(schema["k"], values, precision)
    msg = _message_in(cls, r)
    r.finish()
    return msg


def flatten_stream(
    name: str,
    payload: bytes,
    precision: float | None = None,
    json_payload: bool = False,
) -> Flat:
    """A ZMQ payload: JSON as numbers when asked and it parses, else the raw bytes."""
    if json_payload:
        try:
            tree = json.loads(payload)

            def build_tree(p):
                out = _Out(p)
                _tree_out(tree, out)
                return "j", out.tokens, out.values

            return _with_precision({"z": name}, precision, build_tree)
        except (TypeError, ValueError):
            pass
    return Flat({"z": name}, [base64.b64encode(payload).decode()])


def rebuild_stream(schema: dict, values: list) -> bytes:
    if "j" in schema:
        r = _In(schema["j"], values, schema.get("p"))
        tree = _tree_in(r)
        r.finish()
        return json.dumps(tree, separators=(",", ":")).encode()
    (raw,) = values
    return base64.b64decode(raw)
