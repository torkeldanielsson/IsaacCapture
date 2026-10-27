<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Teleop over Voysys Oden

Carries Isaac Teleop's robot data between the workstation and the robot over
[Voysys Oden](https://voysys.se), so that one Oden link holds everything, over
any network Oden runs on: the robot's cameras (camera_viz `type: oden`, see
[camera streaming](../../docs/source/references/camera_streaming.rst)), the
operator's commands, and the robot's state.

```text
workstation                                          robot
teleop_ros2 node --ROS 2--> operator_bridge          robot_bridge --ROS 2--> robot controllers
vehicle worker  --ZMQ-----> (OCP, Oden Player) ====> (OCP, Oden Streamer) --ZMQ--> Panda worker
camera_viz <--CUDA IPC---- Oden Player <======video== Oden Streamer <-- cameras
                     <---- operator_bridge <========= robot_bridge <-- /joint_states
```

Both bridges talk to Oden Control Pipeline (OCP), the control channel in the Oden
Player and Streamer: `operator_bridge` to the Player's OCP TCP server (port 4001),
`robot_bridge` to the Streamer's (port 4000). Which topics and ZMQ streams cross is
set by a YAML file shared by both ends: [`config/teleop_ros2.yaml`](config/teleop_ros2.yaml)
for the teleop_ros2 reference publisher, [`config/vehicle_teleop.yaml`](config/vehicle_teleop.yaml)
for the vehicle_teleop example.

## Timestamps

Every stamp crosses in the sender's clock and is moved into the receiver's: each
`builtin_interfaces/Time` in a message (headers, every `/tf` transform), each
configured msgpack key (`timestamp` in `controller_data` and `full_body`) and each
configured ZMQ JSON field (`timestamp_ns` in vehicle commands). The offset between
the two clocks is measured over OCP itself, NTP style, from the trips the bridges'
messages make anyway; it needs no NTP or PTP between the machines. Its error is at
most half the round trip of the sample it came from, reported as
`clock_error_bound_ms` on `/oden_teleop/status`. Uneven delays in the two
directions use up that bound, as with NTP.

## Safety

The robot bridge republishes a sample only once, and drops it while OCP reports a
fault (`stop_on_ocp_fault`), while the clock offset is unknown, and when it is
older than `max_age_ms` (default 500, OCP's own latency limit) in the robot's
clock. When the operator's data stops, nothing more is published, so robot-side
timeouts work as they do on a local network. OCP raises a fault within half a
second of losing the operator bridge, the Player or the link.

## Running it

Both machines run Oden 0.26 or later with the OCP plugin (`libocp.so`) next to the
application. The `oden-vr` and `oden-fleet-streamer` packages ship it; with plain
`oden-streamer`, copy it from `oden-fleet-streamer` of the same version. The Player
starts with OCP off: set `disabled : false;` in
`~/.config/oden/OdenVR/libocp_2de7c4c4-8cb6-48f1-a59d-bc3007d9df22.conf`.

Build the bridge image on each machine from the repository root:

```bash
docker build -f examples/teleop_oden/Dockerfile -t teleop_oden .
```

Keep the two ROS 2 graphs apart, so that data cannot also reach the robot directly
over the network: local discovery on both, or different `ROS_DOMAIN_ID`s.

```bash
# robot
docker run --rm --net=host --ipc=host -e ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST \
  teleop_oden python3 -m isaaccapture_examples.teleop_oden.robot_bridge \
  --ros-args -p config:=/opt/teleop_oden/config/teleop_ros2.yaml

# workstation, next to the teleop_ros2 container (same discovery settings)
docker run --rm --net=host --ipc=host -e ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST \
  teleop_oden python3 -m isaaccapture_examples.teleop_oden.operator_bridge \
  --ros-args -p config:=/opt/teleop_oden/config/teleop_ros2.yaml
```

`/oden_teleop/status` (`diagnostic_msgs/DiagnosticArray`) on both ends reports the
OCP state, clock offset, sample ages, rates and drops; `-p stats_file:=PATH`
appends the same as JSON lines.

## Limits

- OCP forwards the newest control data once per Player frame, and skips a frame that
  comes less than 10 ms after the last one. Commands therefore cross at the Player's
  frame rate: 30 Hz for a headless Player by default, about 80 Hz with
  `use_idle_framerate_cap : true; idle_framerate_cap : 90.0;` in the project. A cap of
  100 Hz halves the rate, as most 100 Hz frames are under 10 ms apart.
- The Streamer's OCP reads at most 4 KiB per control message. Numbers cross packed
  (float32 for the teleop topics) and names only until the robot acknowledges them,
  so all of hand_teleop's topics fit; `budget_bytes` keeps the user data under the
  limit, and topics later in the config wait when it is full.
- Every new sample is resent until the robot acknowledges it. At about 80 sends per
  second that is roughly 0.7 Mbit/s towards the robot in controller_teleop and
  1.9 Mbit/s in hand_teleop (the raw hand joints are most of it); at 30 Hz, about
  a third of that.

## Measured

2026-10-27, office LAN, Jetson AGX Thor robot (Oden Streamer nightly 0.26.1027)
and an RTX 4070 workstation (Oden Player), teleop_ros2 replaying the MCAP fixture,
Player at 90 Hz unless noted, video crossing the same link. Delay is from the
teleop_ros2 publish to delivery on the robot; stamp error is against an
independent LAN clock measurement (0.1 ms round trip).

| Case | Delay p50 / p95 / max | Stamp error |
|---|---|---|
| hand_teleop, all topics incl. 52-joint hand | 11–15 / 28–31 / 38 ms | 3.5–4.4 ms |
| hand_teleop, Player 30 Hz | 20–24 / 37–41 / 50 ms | 2.0–4.0 ms |
| full_body | 9–10 / 18–19 / 27 ms | 2.4–3.1 ms |
| hand_teleop, 25 ms added each way, 1 % loss | 35–39 / 48–52 / 64 ms | 2.4–3.9 ms |
| vehicle_teleop ZMQ commands to the Panda worker | 7 / 12 / 24 ms | 3.1–3.4 ms |

Every message delivered on the robot equalled the workstation's to float32 (`/tf`
crosses merged per frame). Stopping the operator bridge or killing the Player
stopped delivery at once and raised an OCP fault within 0.5 s; delivery resumed
1.4 s after the Player restarted.
