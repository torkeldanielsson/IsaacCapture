# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""What both bridge ends share: the configured topics and streams in both
directions, the outbox of local samples and the inbox of the peer's."""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Callable

from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy

from .codec import (
    flatten_stream,
    flatten_topic,
    message_class,
    rebuild_stream,
    rebuild_topic,
)
from .common import (
    STATUS_TOPIC,
    TF_TYPE,
    PeriodStats,
    Rates,
    TfMerger,
    TfRepeatFilter,
    status_message,
)
from .config import TopicSpec, ZmqSpec, load_config
from .envelope import Inbox, Outbox, Received
from .zmq_streams import ZmqSink, ZmqSource

# Larger than any envelope OCP takes; the CBOR budget is the one that binds.
JSON_BUDGET = 15000
_NOT_CARRIED = {"/rosout", "/parameter_events", f"/{STATUS_TOPIC}"}

# Gate(channel key, hold_ms) -> whether to deliver a received sample.
Gate = Callable[[str, int], bool]


def _sub_qos(spec: TopicSpec) -> QoSProfile:
    if spec.latched:
        return QoSProfile(
            depth=1,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
        )
    # Best effort takes both reliable and best-effort publishers; only the newest
    # sample matters.
    return QoSProfile(depth=1, reliability=ReliabilityPolicy.BEST_EFFORT)


def _pub_qos(spec: TopicSpec) -> QoSProfile:
    if spec.latched:
        return QoSProfile(
            depth=1,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
        )
    return QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE)


class Bridge(Node):
    def __init__(
        self, node_name: str, send: str, receive: str, budget: int, **node_kwargs
    ):
        super().__init__(node_name, **node_kwargs)
        config_path = self.declare_parameter("config", "").value
        budget = self.declare_parameter("budget_bytes", budget).value
        status_period = self.declare_parameter("status_period_s", 1.0).value
        discovery_period = self.declare_parameter("discovery_period_s", 1.0).value
        stats_file = self.declare_parameter("stats_file", "").value
        if not config_path:
            raise ValueError("parameter 'config' (bridge YAML) is required")
        self._config = load_config(config_path)
        self._stats_file = Path(stats_file) if stats_file else None

        self._lock = threading.Lock()
        self._outbox = Outbox(budget, JSON_BUDGET)
        self._inbox = Inbox()
        self._send_specs = self._config.topics(send)
        self._receive_specs = self._config.topics(receive)
        self._subscribed: dict[str, str] = {}
        self._unusable: set[str] = set()
        self._outputs: dict[str, tuple[str, object]] = {}
        self._tf_mergers: dict[str, TfMerger] = {}
        self._tf_filters: dict[str, TfRepeatFilter] = {}
        self._ages = PeriodStats()
        self._counts: Counter[str] = Counter()
        self._warned: set[str] = set()
        self._rates = Rates()
        self._discovered = 0

        for spec in self._receive_specs:
            if spec.type and not spec.is_pattern:
                self._publisher(spec.topic, spec.type, spec)
        for index, spec in enumerate(self._send_specs):
            if spec.type and not spec.is_pattern:
                self._subscribe(spec.topic, spec.type, spec, (0, index, 0))
        if any(s.is_pattern or not s.type for s in self._send_specs):
            self.create_timer(discovery_period, self._discover)

        self._zmq_sinks = {
            z.name: ZmqSink(z) for z in self._config.zmq_streams(receive)
        }
        self._zmq_sources = []
        for index, spec in enumerate(self._config.zmq_streams(send)):
            self._outbox.add_channel(f"z:{spec.name}", (1, index, 0), spec.max_rate_hz)
            self._zmq_sources.append(
                ZmqSource(
                    spec, lambda payload, spec=spec: self._on_local_zmq(spec, payload)
                )
            )

        self._status_pub = self.create_publisher(
            message_class("diagnostic_msgs/msg/DiagnosticArray"), STATUS_TOPIC, 10
        )
        self.create_timer(status_period, self._publish_status)

    def _start_streams(self) -> None:
        """Start reading local ZMQ streams; the subclass calls this once it is ready."""
        for source in self._zmq_sources:
            source.start()

    def destroy_node(self):
        for source in self._zmq_sources:
            source.close()
        for sink in self._zmq_sinks.values():
            sink.close()
        return super().destroy_node()

    # Local data going to the peer.

    def _subscribe(self, topic: str, type_name: str, spec: TopicSpec, priority) -> None:
        try:
            cls = message_class(type_name)
        except (ImportError, AttributeError, ValueError) as e:
            self._unusable.add(topic)
            self._warn_once(
                f"type:{type_name}", f"cannot carry {topic}: {type_name} ({e})"
            )
            return
        self._subscribed[topic] = type_name
        if type_name == TF_TYPE:
            self._tf_mergers[topic] = TfMerger()
        with self._lock:
            self._outbox.add_channel(f"t:{topic}", priority, spec.max_rate_hz)
        self.create_subscription(
            cls,
            topic,
            lambda msg: self._on_local_topic(spec, topic, type_name, msg),
            _sub_qos(spec),
        )
        self.get_logger().info(f"carrying {topic} ({type_name})")

    def _discover(self) -> None:
        """Subscribe to graph topics matching a pattern or untyped entry."""
        for topic, types in self.get_topic_names_and_types():
            if topic in self._subscribed or topic in self._outputs:
                continue
            if topic in _NOT_CARRIED or topic in self._unusable:
                continue
            for index, spec in enumerate(self._send_specs):
                if not (spec.is_pattern or not spec.type):
                    continue
                if not spec.is_pattern and topic != spec.topic:
                    continue
                candidates = [t for t in types if spec.matches(topic, t)]
                if len(candidates) != 1:
                    if candidates:
                        self._warn_once(
                            f"types:{topic}",
                            f"{topic} has several types {types}; set 'type'",
                        )
                    continue
                self._discovered += 1
                self._subscribe(
                    topic, candidates[0], spec, (0, index, self._discovered)
                )
                break

    def _on_local_topic(self, spec: TopicSpec, topic: str, type_name: str, msg) -> None:
        if topic in self._tf_mergers:
            msg = self._tf_mergers[topic].merge(msg)
        flat = flatten_topic(
            topic, type_name, msg, spec.precision, spec.payload == "msgpack"
        )
        with self._lock:
            self._outbox.put(f"t:{topic}", flat, time.monotonic_ns())
            self._local_sample()

    def _on_local_zmq(self, spec: ZmqSpec, payload: bytes) -> None:
        flat = flatten_stream(
            spec.name, payload, spec.precision, spec.payload == "json"
        )
        with self._lock:
            self._outbox.put(f"z:{spec.name}", flat, time.monotonic_ns())
            self._local_sample()

    def _local_sample(self) -> None:
        """A new local sample is in the outbox. Called with the lock held."""

    # Peer data arriving.

    def _accept(self, env: dict, gate: Gate) -> None:
        """Take a peer envelope and deliver its new samples. Call with the lock held."""
        peer = self._inbox.peer_sid
        try:
            received = self._inbox.accept(env)
        except (KeyError, ValueError, TypeError, IndexError) as e:
            self._warn_once("bad-envelope", f"ignoring malformed peer envelope: {e}")
            return
        if self._inbox.peer_sid != peer:
            self._outbox.forget_peer()
            self.get_logger().info(f"peer bridge session {self._inbox.peer_sid}")
        self._outbox.peer_ack(env.get("a"))
        for sample in received:
            if "t" in sample.schema:
                self._deliver_topic(sample, gate)
            else:
                self._deliver_stream(sample, gate)

    def _receive_spec(self, topic: str, type_name: str) -> TopicSpec | None:
        for spec in self._receive_specs:
            if spec.matches(topic, type_name):
                return spec
        return None

    def _publisher(self, topic: str, type_name: str, spec: TopicSpec):
        known = self._outputs.get(topic)
        if known is not None:
            return known[1] if known[0] == type_name else None
        publisher = self.create_publisher(
            message_class(type_name), topic, _pub_qos(spec)
        )
        self._outputs[topic] = (type_name, publisher)
        return publisher

    def _deliver_topic(self, sample: Received, gate: Gate) -> None:
        topic, type_name = sample.schema["t"], sample.schema["y"]
        spec = self._receive_spec(topic, type_name)
        if spec is None:
            self._warn_once(
                f"topic:{topic}",
                f"peer sends {topic} ({type_name}), which this bridge's config does not "
                "take; ignoring it",
            )
            return
        if not gate(sample.key, sample.hold_ms):
            return
        try:
            msg = rebuild_topic(sample.schema, sample.values)
            publisher = self._publisher(topic, type_name, spec)
        except (ImportError, AttributeError, ValueError, TypeError) as e:
            self._warn_once(f"rebuild:{topic}", f"cannot rebuild {topic}: {e}")
            self._counts[f"{sample.key}:malformed"] += 1
            return
        if publisher is None:
            self._warn_once(
                f"pubtype:{topic}", f"{topic} is already published with another type"
            )
            return
        if type_name == TF_TYPE:
            msg = self._tf_filters.setdefault(topic, TfRepeatFilter()).fresh(msg)
            if msg is None:
                return
        publisher.publish(msg)
        self._counts[f"{sample.key}:published"] += 1

    def _deliver_stream(self, sample: Received, gate: Gate) -> None:
        name = sample.schema["z"]
        sink = self._zmq_sinks.get(name)
        if sink is None:
            self._warn_once(
                f"zmq:{name}", f"peer sends ZMQ stream {name}, not taken here"
            )
            return
        if not gate(sample.key, sample.hold_ms):
            return
        try:
            payload = rebuild_stream(sample.schema, sample.values)
        except (ValueError, TypeError) as e:
            self._warn_once(f"rebuild:{name}", f"cannot rebuild ZMQ stream {name}: {e}")
            self._counts[f"{sample.key}:malformed"] += 1
            return
        sink.publish(payload)
        self._counts[f"{sample.key}:published"] += 1

    # Status.

    def _warn_once(self, key: str, text: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.get_logger().warning(text)

    def _status(self) -> tuple[bool, str, dict]:
        """(ok, summary, values) of the end-specific state. Called with the lock held."""
        return True, "ok", {}

    def _publish_status(self) -> None:
        with self._lock:
            ok, summary, values = self._status()
            values = {
                **values,
                "carrying": sorted(self._subscribed),
                "envelope_cbor_bytes": self._outbox.last_cbor,
                "dropped_over_budget": dict(self._outbox.dropped),
                "schema_waiting_for_room": dict(self._outbox.schema_waiting),
                "missing_schema": dict(self._inbox.missing_schema),
                "missing_base": dict(self._inbox.missing_base),
                "age": self._ages.take(),
                "counts": dict(self._counts),
            }
        self._status_pub.publish(
            status_message(
                self.get_name(), ok, summary, values, self.get_clock().now().to_msg()
            )
        )
        if self._stats_file is not None:
            with self._stats_file.open("a") as f:
                f.write(json.dumps({"t": time.time_ns(), **values}) + "\n")
