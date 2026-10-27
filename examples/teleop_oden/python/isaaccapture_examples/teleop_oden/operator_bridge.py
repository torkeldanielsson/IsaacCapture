# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator end of the Oden bridge: the workstation's ROS 2 graph <-> OCP in the Oden Player.

Sends the newest sample of every ``to_robot`` topic and ZMQ stream through the
Player's OCP TCP server (port 4001), and republishes the robot's ``to_operator``
topics with their stamps moved into this machine's clock. Writes to OCP on every
new sample and on every OCP update, so OCP always forwards current data and its
latency token stays fresh.
"""

from __future__ import annotations

import json
import threading
from collections import Counter, OrderedDict
from pathlib import Path

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile

from .clock_sync import SystemClock
from .common import (
    STATUS_TOPIC,
    TF_TYPE,
    PeriodStats,
    Rates,
    TfMerger,
    TfRepeatFilter,
    status_message,
)
from .config import TO_OPERATOR, TO_ROBOT, TopicSpec, load_config
from .envelope import KEY, Inbox, Outbox
from .flatten import message_class, pack, unpack
from .ocp_link import OcpLink
from .zmq_streams import ZmqSink, ZmqSource, retime, stamp_of

# The robot's OCP reads at most 4 KiB per control message (CBOR, with OCP's own
# fields); this bounds our JSON user data, which encodes no smaller than CBOR.
DEFAULT_BUDGET_BYTES = 3600
# Video latency echoes kept (OCP latency token -> robot realtime when echoed).
_ECHOES_KEPT = 256


class OperatorBridge(Node):
    def __init__(self, clock: SystemClock | None = None, **node_kwargs):
        super().__init__("oden_teleop_operator", **node_kwargs)
        config_path = self.declare_parameter("config", "").value
        host = self.declare_parameter("ocp_host", "127.0.0.1").value
        port = self.declare_parameter("ocp_port", 4001).value
        self._vehicle_param = self.declare_parameter("vehicle", "").value
        budget = self.declare_parameter("budget_bytes", DEFAULT_BUDGET_BYTES).value
        self._disable_gamepad = self.declare_parameter("disable_gamepad", True).value
        status_period = self.declare_parameter("status_period_s", 1.0).value
        stats_file = self.declare_parameter("stats_file", "").value
        if not config_path:
            raise ValueError("parameter 'config' (bridge YAML) is required")
        self._config = load_config(config_path)
        self._stats_file = Path(stats_file) if stats_file else None

        self._time = clock or SystemClock()
        self._lock = threading.Lock()
        self._outbox = Outbox(budget)
        self._inbox = Inbox()
        self._vehicle: str | None = None
        self._token = (0, 0)
        self._roundtrip = None
        self._sync_echo: tuple[int, int, int] | None = None
        self._robot_clock: list[int] | None = None
        self._echo_times: OrderedDict[int, int] = OrderedDict()
        self._last_frame_token = None
        self._ocp_view: dict = {}
        self._specs = {s.topic: s for s in self._config.to_operator}
        self._tf_mergers: dict[str, TfMerger] = {}
        self._tf_filters: dict[str, TfRepeatFilter] = {}
        self._ages = PeriodStats()
        self._counts: Counter[str] = Counter()
        self._warned: set[str] = set()
        self._rates = Rates(self._time)

        for spec in self._config.to_robot:
            self._outbox.add_channel(spec.topic)
            if spec.type == TF_TYPE:
                self._tf_mergers[spec.topic] = TfMerger()
            self.create_subscription(
                message_class(spec.type),
                spec.topic,
                lambda msg, spec=spec: self._on_local_topic(spec, msg),
                QoSProfile(depth=1),
            )
        self._zmq_sources = []
        for spec in self._config.zmq_streams(TO_ROBOT):
            self._outbox.add_channel(spec.name)
            source = ZmqSource(
                spec,
                lambda payload, now, spec=spec: self._on_local_zmq(spec, payload, now),
                self._time,
            )
            source.start()
            self._zmq_sources.append(source)
        qos = QoSProfile(depth=10)
        self._outputs = {
            s.topic: self.create_publisher(message_class(s.type), s.topic, qos)
            for s in self._config.to_operator
        }
        self._zmq_sinks = {
            z.name: ZmqSink(z) for z in self._config.zmq_streams(TO_OPERATOR)
        }

        self._status_pub = self.create_publisher(
            message_class("diagnostic_msgs/msg/DiagnosticArray"), STATUS_TOPIC, 10
        )
        self.create_timer(status_period, self._publish_status)
        self._link = OcpLink(
            host,
            port,
            self._on_feedback,
            self.get_logger().info,
            self.get_logger().warning,
            self._time,
        )
        self._link.start()

    def destroy_node(self):
        self._link.close()
        for source in self._zmq_sources:
            source.close()
        for sink in self._zmq_sinks.values():
            sink.close()
        return super().destroy_node()

    # Workstation data going to the robot.

    def _on_local_topic(self, spec: TopicSpec, msg) -> None:
        now = self._time.realtime_ns()
        if spec.topic in self._tf_mergers:
            msg = self._tf_mergers[spec.topic].merge(msg)
        packed = pack(spec, msg)
        with self._lock:
            self._outbox.put_ros(spec.topic, packed, packed.stamp_ns or now)
            self._write()

    def _on_local_zmq(self, spec, payload: str, now: int) -> None:
        with self._lock:
            self._outbox.put_zmq(
                spec.name, payload, stamp_of(payload, spec.timestamp_field) or now
            )
            self._write()

    def _write(self) -> None:
        """Hand OCP the newest of everything. Call with the lock held."""
        if self._vehicle is None:
            return
        now, now_mono = self._time.realtime_ns(), self._time.monotonic_ns()
        extra = {"ack": self._inbox.ack()}
        if self._sync_echo is not None:
            t1, t2, t2_mono = self._sync_echo
            extra["sync"] = [t1, t2, now_mono - t2_mono]
        env = self._outbox.build(now, extra)
        ack_time, ack_time_mac = self._token
        self._link.send(
            {
                "active_vehicle": self._vehicle,
                "client_user_data": {
                    self._vehicle: {
                        "user_data": {KEY: env},
                        "ocp_disable_gamepad": self._disable_gamepad,
                        "ack_time_returned": ack_time,
                        "ack_time_mac_returned": ack_time_mac,
                        "roundtrip": self._roundtrip,
                    }
                },
            }
        )

    # OCP.

    def _pick_vehicle(self, feedback: dict) -> str | None:
        if self._vehicle_param:
            return self._vehicle_param if self._vehicle_param in feedback else None
        controlled = [name for name, fb in feedback.items() if not fb.get("is_monitor")]
        if len(controlled) != 1:
            self._warn_once(
                f"vehicles:{sorted(controlled)}",
                f"OCP lists vehicles {sorted(controlled)}; set the 'vehicle' parameter",
            )
            return None
        return controlled[0]

    def _on_feedback(self, msg: dict, now: int, now_mono: int) -> None:
        feedback = msg.get("vehicle_feedback") or {}
        with self._lock:
            vehicle = self._pick_vehicle(feedback)
            if vehicle != self._vehicle:
                self.get_logger().info(f"sending to OCP vehicle {vehicle!r}")
                self._vehicle = vehicle
            if vehicle is None:
                return
            fb = feedback[vehicle]
            self._token = (fb.get("ack_time", 0), fb.get("ack_time_mac", 0))
            self._ocp_view = {
                "ping_ms": fb.get("ping_latency_ms"),
                "loop_latency_ms": fb.get("message_latency_roundtrip"),
                "operator_faults": fb.get("sender_fault"),
                "robot_faults": fb.get("receiver_fault"),
                "ms_since_robot_data": fb.get("ms_since_last_vehicle_data"),
            }
            data = fb.get("vehicle_data") or {}
            roundtrip = data.get("roundtrip")
            if isinstance(roundtrip, dict) and roundtrip != self._roundtrip:
                self._roundtrip = roundtrip
                if isinstance(roundtrip.get("t1"), int):
                    self._sync_echo = (roundtrip["t1"], now, now_mono)
            robot_env = (data.get("user_data") or {}).get(KEY)
            if robot_env is not None:
                self._on_robot_envelope(robot_env, now)
            self._measure_video(fb.get("ack_time"), now)
            self._write()

    def _to_operator(self, robot_ns: int) -> int:
        return robot_ns + self._robot_clock[0]

    def _on_robot_envelope(self, env: dict, now: int) -> None:
        robot_sid = self._inbox.peer_sid
        try:
            new, ros, zmq = self._inbox.accept(env)
        except (KeyError, ValueError, TypeError) as e:
            self._warn_once("bad-envelope", f"ignoring malformed robot envelope: {e}")
            return
        if self._inbox.peer_sid != robot_sid:
            self._outbox.forget_peer()
            self._robot_clock = None
            self.get_logger().info(f"robot bridge session {self._inbox.peer_sid}")
        if not new:
            return
        self._outbox.peer_ack(env.get("ack"))
        if env.get("clock"):
            self._robot_clock = env["clock"]
        for token, t_robot in env.get("echo", ()):
            self._echo_times[token] = t_robot
        while len(self._echo_times) > _ECHOES_KEPT:
            self._echo_times.popitem(last=False)
        if self._robot_clock is None:
            self._counts["from_robot:dropped_no_clock"] += len(ros) + len(zmq)
            return
        for sample in ros:
            topic = sample.schema["topic"]
            spec = self._specs.get(topic)
            if spec is None or sample.schema["type"] != spec.type:
                self._warn_once(
                    f"topic:{topic}",
                    f"robot sends {topic} ({sample.schema['type']}), not in this "
                    "bridge's to_operator list with that type; ignoring it",
                )
                continue
            self._ages.add(topic, now - self._to_operator(sample.stamp_ns))
            out = unpack(sample.schema, sample.values, self._to_operator)
            if spec.type == TF_TYPE:
                out = self._tf_filters.setdefault(topic, TfRepeatFilter()).fresh(out)
                if out is None:
                    continue
            self._outputs[topic].publish(out)
            self._counts[f"{topic}:published"] += 1
        for sample in zmq:
            sink = self._zmq_sinks.get(sample.name)
            if sink is None:
                self._warn_once(
                    f"zmq:{sample.name}", f"ZMQ stream {sample.name} not configured"
                )
                continue
            spec = next(z for z in self._config.zmq if z.name == sample.name)
            self._ages.add(
                f"zmq:{sample.name}", now - self._to_operator(sample.stamp_ns)
            )
            sink.publish(
                retime(sample.payload, spec.timestamp_field, self._to_operator)
            )
            self._counts[f"zmq:{sample.name}:published"] += 1

    def _measure_video(self, token, now: int) -> None:
        """Robot-to-operator video latency: OCP embeds its latency token in every frame,
        and the robot reports when it echoed each token."""
        if token is None or token == self._last_frame_token:
            return
        self._last_frame_token = token
        t_robot = self._echo_times.get(token)
        if t_robot is not None and self._robot_clock is not None:
            self._ages.add("video", now - self._to_operator(t_robot))

    # Status.

    def _warn_once(self, key: str, text: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.get_logger().warning(text)

    def _publish_status(self) -> None:
        with self._lock:
            ages = self._ages.take()
            counts = dict(self._counts)
            values = {
                "ocp_connected": self._link.connected,
                "rates_hz": self._rates.take(
                    ocp_rx=self._link.frames_in, ocp_tx=self._link.frames_out
                ),
                "vehicle": self._vehicle or "",
                **{f"ocp_{k}": v for k, v in self._ocp_view.items()},
                "clock_offset_ms": None
                if self._robot_clock is None
                else round(self._robot_clock[0] / 1e6, 3),
                "clock_error_bound_ms": None
                if self._robot_clock is None
                else round(self._robot_clock[1] / 2e6, 3),
                "envelope_bytes": self._outbox.last_size,
                "dropped_over_budget": dict(self._outbox.dropped),
                "schema_waiting_for_room": dict(self._outbox.schema_waiting),
                "age": ages,
                "counts": counts,
            }
        ok = bool(
            self._link.connected and self._vehicle and self._robot_clock is not None
        )
        if not self._link.connected:
            summary = "no OCP connection"
        elif not self._vehicle:
            summary = "no OCP vehicle"
        elif self._robot_clock is None:
            summary = "waiting for clock sync"
        else:
            summary = "ok"
        self._status_pub.publish(
            status_message(
                self.get_name(), ok, summary, values, self.get_clock().now().to_msg()
            )
        )
        if self._stats_file is not None:
            with self._stats_file.open("a") as f:
                f.write(json.dumps({"t": self._time.realtime_ns(), **values}) + "\n")


def main() -> int:
    rclpy.init()
    node = None
    try:
        node = OperatorBridge()
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if node is not None:
            node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
