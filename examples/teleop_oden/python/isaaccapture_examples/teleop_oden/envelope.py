# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""The envelope each bridge puts in OCP user data, and its sending and receiving ends.

OCP keeps only the newest user data, sends it once per Oden frame and stores it
as CBOR on the wire, so the envelope is plain JSON of small numbers. Every
envelope carries the newest sample of every channel until the receiver
acknowledges it. Integer values go as the difference to a sample the receiver
has acknowledged, so values that change little cost about a byte, and a run of
three or more zeros goes as ``null, count``. Fields::

    v    format version
    id   sender session id (new on every start)
    c    [[schema number, schema], ...]          schemas the receiver lacks
    s    [[schema number, seq, back, hold_ms, value, ...], ...]
         seq counts the channel's samples modulo 2^16; the values are relative to
         the sample ``back`` before (0: absolute); hold_ms is how long the sample
         had waited on the sending machine when the envelope was built
    a    [peer id, schema number, seq or null, ...]
         the newest schema and sample this side holds of each of the peer's channels
"""

from __future__ import annotations

import json
import random
import struct
from collections import Counter, OrderedDict
from dataclasses import dataclass, field

from .codec import Flat, channel_key, delta, undelta

VERSION = 2
# Samples kept per channel at both ends as bases for differences. The sender
# only uses acknowledged ones, so this covers acknowledgements up to this many
# samples late.
HISTORY = 256
MAX_SCHEMAS = 1024
_SEQ = 0xFFFF


def cbor_size(value) -> int:
    """Bytes of ``value`` as OCP's CBOR encoder (ciborium) writes it: shortest
    integers and the shortest float that keeps the value exact."""
    if value is None or value is True or value is False:
        return 1
    if type(value) is int:
        return _head(value if value >= 0 else -1 - value)
    if type(value) is float:
        for code, size in (("<e", 3), ("<f", 5)):
            try:
                if struct.unpack(code, struct.pack(code, value))[0] == value:
                    return size
            except (OverflowError, struct.error):
                pass
        return 9
    if type(value) is str:
        n = len(value.encode())
        return _head(n) + n
    if isinstance(value, (list, tuple)):
        return _head(len(value)) + sum(cbor_size(v) for v in value)
    if isinstance(value, dict):
        return _head(len(value)) + sum(
            cbor_size(k) + cbor_size(v) for k, v in value.items()
        )
    raise TypeError(f"no CBOR size for {type(value).__name__}")


def _head(n: int) -> int:
    return (
        1
        if n <= 23
        else 2
        if n <= 0xFF
        else 3
        if n <= 0xFFFF
        else 5
        if n <= 0xFFFFFFFF
        else 9
    )


def json_size(value) -> int:
    return len(json.dumps(value, separators=(",", ":")))


def newer(seq: int, than: int) -> bool:
    return 0 < (seq - than) & _SEQ < 0x8000


def squeeze(values: list) -> list:
    """Runs of three or more integer zeros as ``None, count``."""
    out: list = []
    i, n = 0, len(values)
    while i < n:
        j = i
        while j < n and type(values[j]) is int and values[j] == 0:
            j += 1
        if j - i >= 3:
            out += [None, j - i]
        elif j > i:
            out += [0] * (j - i)
        else:
            out.append(values[i])
            j += 1
        i = j
    return out


def expand(wire: list) -> list:
    out: list = []
    items = iter(wire)
    for v in items:
        if v is None:
            count = next(items, None)
            if type(count) is not int or count < 0:
                raise ValueError("bad zero run")
            out += [0] * count
        else:
            out.append(v)
    return out


@dataclass
class _Sample:
    number: int
    seq: int
    values: list
    t_ns: int


@dataclass
class _Channel:
    priority: tuple
    min_interval_ns: int = 0
    seq: int = 0
    current: _Sample | None = None
    # A newer sample waiting for min_interval_ns to pass: (flat, put time).
    waiting: tuple | None = None
    history: OrderedDict = field(default_factory=OrderedDict)
    # (sample seq, base seq) -> (entry values, their CBOR bytes)
    cached: tuple | None = None


class Outbox:
    """Newest sample per channel; ``build`` packs as many as fit, in priority order."""

    def __init__(self, cbor_budget: int, json_budget: int):
        self.session_id = random.randrange(1, 2**31)
        self.cbor_budget = cbor_budget
        self.json_budget = json_budget
        # Samples left out for lack of room, and schemas still waiting for room.
        self.dropped: Counter[str] = Counter()
        self.schema_waiting: Counter[str] = Counter()
        self.last_cbor = self.last_json = 0
        # CBOR bytes of each channel's schema and sample in the last envelope.
        self.last_channel_bytes: dict[str, int] = {}
        self._channels: dict[str, _Channel] = {}
        self._numbers: OrderedDict[str, int] = OrderedDict()
        self._schemas: dict[int, dict] = {}
        self._next_number = 1
        self._peer: dict[str, tuple[int, int | None]] = {}

    def add_channel(
        self, key: str, priority: tuple, max_rate_hz: float | None = None
    ) -> None:
        """Register a channel; lower priorities win when the budget is short."""
        interval = int(1e9 / max_rate_hz) if max_rate_hz else 0
        self._channels[key] = _Channel(priority, interval)
        self._channels = dict(
            sorted(self._channels.items(), key=lambda kv: kv[1].priority)
        )

    def _number(self, schema: dict) -> int:
        text = json.dumps(schema, sort_keys=True, separators=(",", ":"))
        number = self._numbers.get(text)
        if number is None:
            number = self._next_number
            self._next_number += 1
            self._numbers[text] = number
            self._schemas[number] = schema
            current = {c.current.number for c in self._channels.values() if c.current}
            while len(self._numbers) > MAX_SCHEMAS:
                old_text, old = next(iter(self._numbers.items()))
                if old in current:
                    self._numbers.move_to_end(old_text)
                    continue
                del self._numbers[old_text]
                del self._schemas[old]
        else:
            self._numbers.move_to_end(text)
        return number

    def put(self, key: str, flat: Flat, now_ns: int) -> None:
        ch = self._channels[key]
        if ch.current and now_ns - ch.current.t_ns < ch.min_interval_ns:
            ch.waiting = (flat, now_ns)
            return
        self._take(ch, flat, now_ns)

    def _take(self, ch: _Channel, flat: Flat, t_ns: int) -> None:
        ch.waiting = None
        ch.seq += 1
        ch.current = _Sample(self._number(flat.schema), ch.seq, flat.values, t_ns)
        ch.history[ch.seq] = ch.current
        while len(ch.history) > HISTORY:
            ch.history.popitem(last=False)

    def forget_peer(self) -> None:
        """The peer restarted: it holds none of our schemas or samples."""
        self._peer = {}

    def peer_ack(self, ack) -> None:
        """Take the peer's ``a`` field: what it holds of our channels."""
        if not ack or ack[0] != self.session_id:
            return
        view = {}
        for i in range(1, len(ack) - 1, 2):
            schema = self._schemas.get(ack[i])
            if schema is not None:
                view[channel_key(schema)] = (ack[i], ack[i + 1])
        self._peer = view

    def _entry(self, ch: _Channel, held: tuple | None, now_ns: int) -> tuple[list, int]:
        cur = ch.current
        base = None
        if held and held[0] == cur.number and held[1] is not None:
            base_seq = cur.seq - ((cur.seq - held[1]) & _SEQ)
            base = ch.history.get(base_seq)
            if base is not None and base.number != cur.number:
                base = None
        base_seq = base.seq if base else 0
        if ch.cached and ch.cached[0] == (cur.seq, base_seq):
            values, size = ch.cached[1]
        else:
            values = None
            if base is not None and len(base.values) == len(cur.values):
                values = delta(cur.values, base.values)
            if values is None:
                base_seq = 0
                values = cur.values
            values = squeeze(values)
            size = sum(cbor_size(v) for v in values)
            ch.cached = ((cur.seq, base_seq), (values, size))
        hold_ms = min(max(0, -(-(now_ns - cur.t_ns) // 1_000_000)), 0xFFFF)
        head = [
            cur.number,
            cur.seq & _SEQ,
            (cur.seq - base_seq) if base_seq else 0,
            hold_ms,
        ]
        entry = head + values
        return entry, _head(len(entry)) + sum(cbor_size(v) for v in head) + size

    def build(self, now_ns: int, ack: list | None) -> dict:
        env: dict = {"v": VERSION, "id": self.session_id}
        if ack:
            env["a"] = ack
        # Keys and array heads of "c" and "s", then a separator per entry below.
        cbor_used = cbor_size(env) + 2 * (2 + 3)
        json_used = json_size(env) + len(',"c":[],"s":[]')
        pending = []
        for key, ch in self._channels.items():
            if ch.waiting and now_ns - ch.current.t_ns >= ch.min_interval_ns:
                self._take(ch, *ch.waiting)
            cur = ch.current
            if cur is None:
                continue
            held = self._peer.get(key)
            if held == (cur.number, cur.seq & _SEQ):
                continue
            pending.append((key, ch, held))
        schemas: list = []
        sent_schemas = set()
        channel_bytes: Counter[str] = Counter()
        # Schemas the peer lacks go first: a sample is useless without its schema.
        for key, ch, held in pending:
            number = ch.current.number
            if (held and held[0] == number) or number in sent_schemas:
                continue
            item = [number, self._schemas[number]]
            c, j = cbor_size(item), json_size(item) + 1
            if cbor_used + c > self.cbor_budget or json_used + j > self.json_budget:
                self.schema_waiting[key] += 1
                continue
            cbor_used, json_used = cbor_used + c, json_used + j
            schemas.append(item)
            sent_schemas.add(number)
            channel_bytes[key] += c
        samples: list = []
        for key, ch, held in pending:
            number = ch.current.number
            if not ((held and held[0] == number) or number in sent_schemas):
                continue
            entry, c = self._entry(ch, held, now_ns)
            j = json_size(entry) + 1
            if cbor_used + c > self.cbor_budget or json_used + j > self.json_budget:
                self.dropped[key] += 1
                continue
            cbor_used, json_used = cbor_used + c, json_used + j
            samples.append(entry)
            channel_bytes[key] += c
        if schemas:
            env["c"] = schemas
        if samples:
            env["s"] = samples
        self.last_cbor, self.last_json = cbor_size(env), json_size(env)
        self.last_channel_bytes = dict(channel_bytes)
        return env


@dataclass(frozen=True)
class Received:
    key: str
    schema: dict
    values: list
    hold_ms: int


@dataclass
class _Held:
    schema_number: int | None = None
    sample_number: int | None = None
    seq: int | None = None


class Inbox:
    """Schemas and recent samples of the peer's current session."""

    def __init__(self):
        self.peer_sid: int | None = None
        self.missing_schema: Counter[str] = Counter()
        self.missing_base: Counter[str] = Counter()
        self._reset()

    def _reset(self) -> None:
        self._schemas: OrderedDict[int, dict] = OrderedDict()
        self._held: dict[str, _Held] = {}
        self._history: dict[int, OrderedDict[int, list]] = {}

    def ack(self) -> list | None:
        if self.peer_sid is None:
            return None
        out: list = [self.peer_sid]
        for held in self._held.values():
            if held.schema_number is not None:
                same = held.sample_number == held.schema_number
                out += [held.schema_number, held.seq if same else None]
        return out

    def accept(self, env: dict) -> list[Received]:
        """The envelope's samples this side does not hold yet, with absolute values.

        OCP repeats its newest message until the next one, so most envelopes are
        repeats; a repeat yields nothing.
        """
        if env.get("v") != VERSION:
            raise ValueError(f"unsupported envelope version {env.get('v')!r}")
        if env["id"] != self.peer_sid:
            self.peer_sid = env["id"]
            self._reset()
        for number, schema in env.get("c", ()):
            if number not in self._schemas:
                self._schemas[number] = schema
            # The sender sends the schema of its current sample, which may be one it
            # used before (a topic switching back to an earlier layout).
            self._held.setdefault(channel_key(schema), _Held()).schema_number = number
        while len(self._schemas) > MAX_SCHEMAS:
            number, _ = self._schemas.popitem(last=False)
            self._history.pop(number, None)
        out = []
        for entry in env.get("s", ()):
            number, seq, back, hold_ms = entry[:4]
            wire = expand(entry[4:])
            schema = self._schemas.get(number)
            if schema is None:
                self.missing_schema[str(number)] += 1
                continue
            key = channel_key(schema)
            held = self._held[key]
            if held.seq is not None and not newer(seq, held.seq):
                continue
            history = self._history.setdefault(number, OrderedDict())
            if back:
                base = history.get((seq - back) & _SEQ)
                if base is None or len(base) != len(wire):
                    self.missing_base[key] += 1
                    continue
                values = undelta(wire, base)
            else:
                values = wire
            history[seq] = values
            while len(history) > HISTORY:
                history.popitem(last=False)
            held.schema_number = held.sample_number = number
            held.seq = seq
            out.append(Received(key, schema, values, hold_ms))
        return out
