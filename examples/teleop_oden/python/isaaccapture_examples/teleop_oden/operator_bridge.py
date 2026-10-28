# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""Operator end of the Oden bridge: the workstation's ROS 2 graph <-> OCP in the Oden Player.

Sends the newest sample of every ``to_robot`` topic and ZMQ stream through the
Player's OCP TCP server (port 4001), and republishes the robot's ``to_operator``
topics and streams. Writes to OCP on every new sample and on every OCP update,
so OCP always forwards current data and its latency token stays fresh.
"""

from __future__ import annotations

import time
from collections import Counter

import rclpy

from .bridge import Bridge
from .config import TO_OPERATOR, TO_ROBOT
from .ocp_link import OcpLink

# The robot's OCP reads at most 4 KiB per control message, CBOR with OCP's own
# fields (about 170 bytes) included.
DEFAULT_BUDGET_BYTES = 3700
# OCP's Player sends no control message within 10 ms of the last one.
_OCP_MIN_SEND_INTERVAL_NS = 10_000_000


class OperatorBridge(Bridge):
    def __init__(self, **node_kwargs):
        super().__init__(
            "oden_teleop_operator",
            TO_ROBOT,
            TO_OPERATOR,
            DEFAULT_BUDGET_BYTES,
            **node_kwargs,
        )
        host = self.declare_parameter("ocp_host", "127.0.0.1").value
        port = self.declare_parameter("ocp_port", 4001).value
        self._vehicle_param = self.declare_parameter("vehicle", "").value
        self._disable_gamepad = self.declare_parameter("disable_gamepad", True).value
        self._vehicle: str | None = None
        self._token = (0, 0)
        self._ocp_view: dict = {}
        # CBOR bytes of user data OCP sent: the newest envelope, once per Player
        # frame at most every 10 ms.
        self._frame_bytes = 0
        self._channel_bytes: Counter[str] = Counter()
        self._last_send_ns = 0
        self._link = OcpLink(
            host,
            port,
            self._on_feedback,
            self.get_logger().info,
            self.get_logger().warning,
        )
        self._link.start()
        self._start_streams()

    def destroy_node(self):
        self._link.close()
        return super().destroy_node()

    def _local_sample(self) -> None:
        self._write()

    def _write(self) -> None:
        """Hand OCP the newest of everything. Call with the lock held."""
        if self._vehicle is None:
            return
        env = self._outbox.build(time.monotonic_ns(), self._inbox.ack())
        ack_time, ack_time_mac = self._token
        self._link.send(
            {
                "active_vehicle": self._vehicle,
                "client_user_data": {
                    self._vehicle: {
                        "user_data": env,
                        "ocp_disable_gamepad": self._disable_gamepad,
                        "ack_time_returned": ack_time,
                        "ack_time_mac_returned": ack_time_mac,
                    }
                },
            }
        )

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

    def _on_feedback(self, msg: dict) -> None:
        feedback = msg.get("vehicle_feedback") or {}
        with self._lock:
            vehicle = self._pick_vehicle(feedback)
            if vehicle != self._vehicle:
                self.get_logger().info(f"sending to OCP vehicle {vehicle!r}")
                self._vehicle = vehicle
            if vehicle is None:
                return
            now = time.monotonic_ns()
            if now - self._last_send_ns >= _OCP_MIN_SEND_INTERVAL_NS:
                self._last_send_ns = now
                self._frame_bytes += self._outbox.last_cbor
                self._channel_bytes.update(self._outbox.last_channel_bytes)
            fb = feedback[vehicle]
            self._token = (fb.get("ack_time", 0), fb.get("ack_time_mac", 0))
            self._ocp_view = {
                "ping_ms": fb.get("ping_latency_ms"),
                "loop_latency_ms": fb.get("message_latency_roundtrip"),
                "operator_faults": fb.get("sender_fault"),
                "robot_faults": fb.get("receiver_fault"),
                "ms_since_robot_data": fb.get("ms_since_last_vehicle_data"),
            }
            robot_env = (fb.get("vehicle_data") or {}).get("user_data")
            if isinstance(robot_env, dict) and "v" in robot_env:
                self._accept(robot_env, lambda key, hold_ms: True)
            self._write()

    def _status(self):
        rates = self._rates.take(
            ocp_rx=self._link.frames_in,
            ocp_tx=self._link.frames_out,
            user_data=self._frame_bytes,
            **{f"bytes:{k}": v for k, v in self._channel_bytes.items()},
        )
        values = {
            "ocp_connected": self._link.connected,
            "vehicle": self._vehicle or "",
            **{f"ocp_{k}": v for k, v in self._ocp_view.items()},
            "rates_hz": {"ocp_rx": rates["ocp_rx"], "ocp_tx": rates["ocp_tx"]},
            # User data OCP carries to the robot, in total and per channel.
            "to_robot_kbit_s": round(rates["user_data"] * 8 / 1000, 1),
            "to_robot_kbit_s_by_channel": {
                k[len("bytes:") :]: round(v * 8 / 1000, 1)
                for k, v in rates.items()
                if k.startswith("bytes:")
            },
        }
        if not self._link.connected:
            return False, "no OCP connection", values
        if not self._vehicle:
            return False, "no OCP vehicle", values
        return True, "ok", values


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
