# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""Robot end of the Oden bridge: OCP in the Oden Streamer <-> the robot's ROS 2 graph.

Republishes the operator's ``to_robot`` topics and streams, with stamps as sent.
A sample is dropped while OCP reports a fault and when it may be older than
``max_age_ms``: OCP's latency (the round trip of its token, on this machine's
clock) plus how long the sample waited on the workstation bounds its age, so the
two machines' clocks never need to agree. ``to_operator`` topics go the other
way. Answers every OCP control message at once, echoing OCP's latency token, as
OCP requires.
"""

from __future__ import annotations

import time

import rclpy

from .bridge import Bridge
from .config import TO_OPERATOR, TO_ROBOT
from .ocp_link import OcpLink

# OCP takes vehicle messages up to 16 KiB of JSON.
DEFAULT_BUDGET_BYTES = 12000


class RobotBridge(Bridge):
    def __init__(self, **node_kwargs):
        super().__init__(
            "oden_teleop_robot",
            TO_OPERATOR,
            TO_ROBOT,
            DEFAULT_BUDGET_BYTES,
            **node_kwargs,
        )
        host = self.declare_parameter("ocp_host", "127.0.0.1").value
        port = self.declare_parameter("ocp_port", 4000).value
        self._max_age_ms = self.declare_parameter("max_age_ms", 500.0).value
        self._stop_on_fault = self.declare_parameter("stop_on_ocp_fault", True).value
        self._ocp = {"fault": None, "fault_reason": "", "latency_ms": None}
        self._link = OcpLink(
            host,
            port,
            self._on_control,
            self.get_logger().info,
            self.get_logger().warning,
        )
        self._link.start()
        self._start_streams()

    def destroy_node(self):
        self._link.close()
        return super().destroy_node()

    def _on_control(self, msg: dict) -> None:
        telemetry = msg.get("telemetry") or {}
        fault = bool(telemetry.get("fault"))
        latency = telemetry.get("latency")
        env = msg.get("client_user_data")
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
                "latency_ms": latency,
            }
            if isinstance(env, dict) and "v" in env:
                self._accept(
                    env, lambda key, hold_ms: self._gate(key, hold_ms, fault, latency)
                )
            self._respond(msg)

    def _gate(self, key: str, hold_ms: int, fault: bool, latency) -> bool:
        if fault and self._stop_on_fault:
            self._counts[f"{key}:dropped_ocp_fault"] += 1
            return False
        if not isinstance(latency, int):
            self._counts[f"{key}:dropped_no_latency"] += 1
            return False
        age_ms = latency + hold_ms
        if age_ms > self._max_age_ms:
            self._counts[f"{key}:dropped_too_old"] += 1
            return False
        self._ages.add(key, age_ms * 1_000_000)
        return True

    def _respond(self, msg: dict) -> None:
        env = self._outbox.build(time.monotonic_ns(), self._inbox.ack())
        self._link.send(
            {
                "ack_time": msg["ack_time"],
                "ack_time_mac": msg["ack_time_mac"],
                "vehicle_user_data": {"user_data": env},
            }
        )

    def _status(self):
        ocp = self._ocp
        values = {
            "ocp_connected": self._link.connected,
            "rates_hz": self._rates.take(ocp_rx=self._link.frames_in),
            "ocp_fault": ocp["fault"],
            "ocp_fault_reason": ocp["fault_reason"],
            "ocp_latency_ms": ocp["latency_ms"],
        }
        if not self._link.connected:
            return False, "no OCP connection", values
        if ocp["fault"]:
            return False, f"OCP fault: {ocp['fault_reason']}", values
        return True, "ok", values


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
