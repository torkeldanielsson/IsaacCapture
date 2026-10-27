# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Robot end of the Oden bridge: OCP in the Oden Streamer <-> the robot's ROS 2 graph.

Republishes the operator's topics with every stamp moved into the robot's clock.
A sample is dropped while OCP reports a fault, while the clock offset is not yet
known, and when it is older than ``max_age_ms``. ``to_operator`` topics go the
other way. Answers every OCP control message at once, echoing OCP's latency
token, as OCP requires.
"""

from __future__ import annotations

import json
import threading
from collections import Counter, OrderedDict, deque
from pathlib import Path

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile

from .clock_sync import ClockSync, SystemClock
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
from .envelope import KEY, Inbox, Outbox, RosSample, ZmqSample
from .flatten import message_class, pack, unpack
from .ocp_link import OcpLink
from .zmq_streams import ZmqSink, ZmqSource, retime, stamp_of

# Robot send times kept to match the operator's sync echoes (5 s of OCP's 100 Hz).
_SENT_T1_KEPT = 512
# (OCP latency token, robot realtime) pairs sent back for the operator's video latency.
_ECHOES_SENT = 8


class RobotBridge(Node):
    def __init__(self, clock: SystemClock | None = None, **node_kwargs):
        super().__init__("oden_teleop_robot", **node_kwargs)
        config_path = self.declare_parameter("config", "").value
        host = self.declare_parameter("ocp_host", "127.0.0.1").value
        port = self.declare_parameter("ocp_port", 4000).value
        self._max_age_ns = int(self.declare_parameter("max_age_ms", 500.0).value * 1e6)
        self._stop_on_fault = self.declare_parameter("stop_on_ocp_fault", True).value
        window_s = self.declare_parameter("clock_window_s", 10.0).value
        # OCP takes vehicle messages up to 16 KiB.
        budget = self.declare_parameter("budget_bytes", 12000).value
        status_period = self.declare_parameter("status_period_s", 1.0).value
        stats_file = self.declare_parameter("stats_file", "").value
        if not config_path:
            raise ValueError("parameter 'config' (bridge YAML) is required")
        self._config = load_config(config_path)
        self._stats_file = Path(stats_file) if stats_file else None

        self._time = clock or SystemClock()
        self._lock = threading.Lock()
        self._sync = ClockSync(window_ns=int(window_s * 1e9))
        self._inbox = Inbox()
        self._outbox = Outbox(budget)
        self._sent_t1: OrderedDict[int, int] = OrderedDict()
        self._echoes: deque[list[int]] = deque(maxlen=_ECHOES_SENT)
        self._specs: dict[str, TopicSpec] = {s.topic: s for s in self._config.to_robot}
        self._tf_filters: dict[str, TfRepeatFilter] = {}
        self._tf_mergers: dict[str, TfMerger] = {}
        self._ages = PeriodStats()
        self._counts: Counter[str] = Counter()
        self._warned: set[str] = set()
        self._ocp = {"fault": None, "fault_reason": "", "latency_ms": None}
        self._operator_sid = None
        self._rates = Rates(self._time)

        qos = QoSProfile(depth=10)
        self._outputs = {
            s.topic: self.create_publisher(message_class(s.type), s.topic, qos)
            for s in self._config.to_robot
        }
        for spec in self._config.to_operator:
            self._outbox.add_channel(spec.topic)
            if spec.type == TF_TYPE:
                self._tf_mergers[spec.topic] = TfMerger()
            self.create_subscription(
                message_class(spec.type),
                spec.topic,
                lambda msg, spec=spec: self._on_local_topic(spec, msg),
                QoSProfile(depth=1),
            )
        self._zmq_sinks = {
            z.name: ZmqSink(z) for z in self._config.zmq_streams(TO_ROBOT)
        }
        self._zmq_sources = []
        for spec in self._config.zmq_streams(TO_OPERATOR):
            self._outbox.add_channel(spec.name)
            source = ZmqSource(
                spec,
                lambda payload, now, spec=spec: self._on_local_zmq(spec, payload, now),
                self._time,
            )
            source.start()
            self._zmq_sources.append(source)

        self._status_pub = self.create_publisher(
            message_class("diagnostic_msgs/msg/DiagnosticArray"), STATUS_TOPIC, 10
        )
        self.create_timer(status_period, self._publish_status)
        self._link = OcpLink(
            host,
            port,
            self._on_control,
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

    # Robot topics going to the operator.

    def _on_local_topic(self, spec: TopicSpec, msg) -> None:
        now = self._time.realtime_ns()
        if spec.topic in self._tf_mergers:
            msg = self._tf_mergers[spec.topic].merge(msg)
        packed = pack(spec, msg)
        with self._lock:
            self._outbox.put_ros(spec.topic, packed, packed.stamp_ns or now)

    def _on_local_zmq(self, spec, payload: str, now: int) -> None:
        with self._lock:
            self._outbox.put_zmq(
                spec.name, payload, stamp_of(payload, spec.timestamp_field) or now
            )

    # OCP.

    def _on_control(self, msg: dict, t4: int, t4_mono: int) -> None:
        telemetry = msg.get("telemetry") or {}
        fault = bool(telemetry.get("fault"))
        env = (msg.get("client_user_data") or {}).get(KEY)
        with self._lock:
            if fault != self._ocp["fault"]:
                if fault:
                    self.get_logger().warning(
                        f"OCP fault: {telemetry.get('fault_reason')}"
                    )
                elif self._ocp["fault"] is not None:
                    self.get_logger().info("OCP fault cleared")
            self._ocp = {
                "fault": fault,
                "fault_reason": telemetry.get("fault_reason", ""),
                "latency_ms": telemetry.get("latency"),
            }
            if env is not None:
                self._on_envelope(env, fault, t4, t4_mono)
            self._respond(msg)

    def _on_envelope(self, env: dict, fault: bool, t4: int, t4_mono: int) -> None:
        try:
            new, ros, zmq = self._inbox.accept(env)
        except (KeyError, ValueError, TypeError) as e:
            self._warn_once("bad-envelope", f"ignoring malformed envelope: {e}")
            return
        if self._inbox.peer_sid != self._operator_sid:
            self._operator_sid = self._inbox.peer_sid
            self._outbox.forget_peer()
            self.get_logger().info(f"operator bridge session {self._operator_sid}")
        if not new:
            return
        self._counts["envelopes"] += 1
        self._outbox.peer_ack(env.get("ack"))
        sync = env.get("sync")
        if sync:
            t1, t2, hold_ns = sync
            t1_mono = self._sent_t1.get(t1)
            if t1_mono is not None:
                had_estimate = self._sync.estimate(t4_mono) is not None
                self._sync.add(
                    t1, t2, env["t"], t4, (t4_mono - t1_mono) - hold_ns, t4_mono
                )
                estimate = self._sync.estimate(t4_mono)
                if estimate is not None and not had_estimate:
                    self.get_logger().info(
                        f"clock offset to operator {estimate.offset_ns / 1e6:+.2f} ms "
                        f"(within {estimate.rtt_ns / 2e6:.2f} ms)"
                    )
        estimate = self._sync.estimate(t4_mono)
        if estimate is not None:
            self._ages.add("ocp:one_way", t4 - estimate.to_robot(env["t"]))
        for sample in ros:
            self._deliver_ros(sample, estimate, fault, t4)
        for sample in zmq:
            self._deliver_zmq(sample, estimate, fault, t4)

    def _gate(
        self, channel: str, stamp_ns: int, estimate, fault: bool, t4: int
    ) -> bool:
        if fault and self._stop_on_fault:
            self._counts[f"{channel}:dropped_ocp_fault"] += 1
            return False
        if estimate is None:
            self._counts[f"{channel}:dropped_no_clock"] += 1
            return False
        age = t4 - estimate.to_robot(stamp_ns)
        if age > self._max_age_ns:
            self._counts[f"{channel}:dropped_too_old"] += 1
            return False
        self._ages.add(channel, age)
        return True

    def _deliver_ros(self, sample: RosSample, estimate, fault: bool, t4: int) -> None:
        topic = sample.schema["topic"]
        spec = self._specs.get(topic)
        if spec is None or sample.schema["type"] != spec.type:
            self._warn_once(
                f"topic:{topic}",
                f"operator sends {topic} ({sample.schema['type']}), not in this "
                "bridge's to_robot list with that type; ignoring it",
            )
            return
        if not self._gate(topic, sample.stamp_ns, estimate, fault, t4):
            return
        msg = unpack(sample.schema, sample.values, estimate.to_robot)
        if spec.type == TF_TYPE:
            msg = self._tf_filters.setdefault(topic, TfRepeatFilter()).fresh(msg)
            if msg is None:
                return
        self._outputs[topic].publish(msg)
        self._counts[f"{topic}:published"] += 1

    def _deliver_zmq(self, sample: ZmqSample, estimate, fault: bool, t4: int) -> None:
        sink = self._zmq_sinks.get(sample.name)
        if sink is None:
            self._warn_once(
                f"zmq:{sample.name}", f"ZMQ stream {sample.name} not configured here"
            )
            return
        channel = f"zmq:{sample.name}"
        if not self._gate(channel, sample.stamp_ns, estimate, fault, t4):
            return
        spec = next(z for z in self._config.zmq if z.name == sample.name)
        sink.publish(retime(sample.payload, spec.timestamp_field, estimate.to_robot))
        self._counts[f"{channel}:published"] += 1

    def _respond(self, msg: dict) -> None:
        t1 = self._time.realtime_ns()
        self._sent_t1[t1] = self._time.monotonic_ns()
        while len(self._sent_t1) > _SENT_T1_KEPT:
            self._sent_t1.popitem(last=False)
        self._echoes.append([msg["ack_time"], t1])
        estimate = self._sync.estimate(self._time.monotonic_ns())
        extra = {"ack": self._inbox.ack(), "echo": list(self._echoes)}
        if estimate is not None:
            extra["clock"] = [estimate.offset_ns, estimate.rtt_ns]
        env = self._outbox.build(t1, extra)
        self._link.send(
            {
                "ack_time": msg["ack_time"],
                "ack_time_mac": msg["ack_time_mac"],
                "vehicle_user_data": {"user_data": {KEY: env}, "roundtrip": {"t1": t1}},
            }
        )

    # Status.

    def _warn_once(self, key: str, text: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.get_logger().warning(text)

    def _publish_status(self) -> None:
        with self._lock:
            estimate = self._sync.estimate(self._time.monotonic_ns())
            ages = self._ages.take()
            counts = dict(self._counts)
            ocp = dict(self._ocp)
            out_dropped = dict(self._outbox.dropped)
        rates = self._rates.take(
            ocp_rx=self._link.frames_in, new_envelopes=counts.get("envelopes", 0)
        )
        values = {
            "ocp_connected": self._link.connected,
            "rates_hz": rates,
            "ocp_fault": ocp["fault"],
            "ocp_fault_reason": ocp["fault_reason"],
            "ocp_loop_latency_ms": ocp["latency_ms"],
            "clock_offset_ms": None
            if estimate is None
            else round(estimate.offset_ns / 1e6, 3),
            "clock_error_bound_ms": None
            if estimate is None
            else round(estimate.rtt_ns / 2e6, 3),
            "clock_samples": self._sync.accepted,
            "age": ages,
            "counts": counts,
            "dropped_to_operator": out_dropped,
            "missing_schema": dict(self._inbox.missing_schema),
        }
        ok = self._link.connected and not ocp["fault"] and estimate is not None
        if not self._link.connected:
            summary = "no OCP connection"
        elif estimate is None:
            summary = "waiting for clock sync"
        elif ocp["fault"]:
            summary = f"OCP fault: {ocp['fault_reason']}"
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
        node = RobotBridge()
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
