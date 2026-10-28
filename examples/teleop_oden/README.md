<!--
SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
SPDX-License-Identifier: MIT
-->

# Teleop over Voysys Oden

Carries ROS 2 topics and ZMQ streams between the workstation and the robot over
[Voysys Oden](https://voysys.se), so that one Oden link holds everything, over any
network Oden runs on: the robot's cameras (camera_viz `type: oden`, see
[camera streaming](../../docs/source/references/camera_streaming.rst)), the
operator's commands, and the robot's state. Any topic of any message type and any
ZMQ stream can cross, in either direction; the configs here set it up for Isaac
Teleop's teleop_ros2 and vehicle_teleop examples.

```text
workstation                                          robot
teleop_ros2 node --ROS 2--> operator_bridge          robot_bridge --ROS 2--> robot controllers
vehicle worker  --ZMQ-----> (OCP, Oden Player) ====> (OCP, Oden Streamer) --ZMQ--> Panda worker
camera_viz <--CUDA IPC---- Oden Player <======video== Oden Streamer <-- cameras
                     <---- operator_bridge <========= robot_bridge <-- /joint_states
```

Both bridges talk to Oden Control Pipeline (OCP), the control channel in the Oden
Player and Streamer: `operator_bridge` to the Player's OCP TCP server (port 4001),
`robot_bridge` to the Streamer's (port 4000).

## Configuration

One YAML file, shared by both ends, lists what crosses:
[`config/teleop_ros2.yaml`](config/teleop_ros2.yaml) for the teleop_ros2 reference
publisher, [`config/vehicle_teleop.yaml`](config/vehicle_teleop.yaml) for the
vehicle_teleop example.

```yaml
to_robot:
  - topic: /xr_teleop/ee_poses        # a topic name, or a glob such as /xr_teleop/*
    type: teleop_ros2_interfaces/msg/NamedPoseArray   # optional: found on the ROS graph
    precision: 0.0001                 # optional: floats as whole multiples of this
    max_rate_hz: 30                   # optional: send at most this often
  - topic: /xr_teleop/controller_data
    payload: msgpack                  # a ByteMultiArray holding msgpack
  - topic: /tf_static
    latched: true                     # transient-local QoS at both ends
to_operator:
  - topic: /joint_states
zmq:
  - name: vehicle_control
    direction: to_robot               # or to_operator
    topic: vehicle_control            # single-frame "<topic> <payload>" messages
    source: tcp://127.0.0.1:5555      # the producer's PUB socket, on the sending side
    sink: tcp://127.0.0.1:5555        # bound for the consumer, on the receiving side
    payload: json                     # or raw (the default): bytes as they are
```

Each end publishes only what its own copy of the file lists for its direction, so
the robot's file decides what reaches the robot. Order is priority: when a control
message is full, later entries wait for the next one. Message types must be
importable on both ends; the [Dockerfile](Dockerfile) builds
teleop_ros2_interfaces, and other packages go in the same way.

## How data crosses

OCP sends the newest control data once per Player frame, so each topic crosses as
its newest sample, resent until the other end acknowledges it. A message is split
into a schema (topic, type, strings, map keys, sequence lengths), sent until
acknowledged, and its numbers. Integers cross as the difference to a sample the
other end has acknowledged, floats too when the topic has a `precision`, and runs
of unchanged values shrink to two bytes, so a value that changes a little costs
about a byte. OCP stores all of it as CBOR.

Stamps cross as they were sent. Neither the bridges nor OCP need the two machines'
clocks to agree; if robot code compares message stamps with its own clock, run
NTP or PTP between the machines, as on any ROS 2 network.

`/oden_teleop/status` (`diagnostic_msgs/DiagnosticArray`) on both ends reports the
OCP state, rates, drops, sample ages and, on the workstation, the bandwidth towards
the robot in total and per topic (`to_robot_kbit_s`, `to_robot_kbit_s_by_channel`);
`-p stats_file:=PATH` appends the same as JSON lines.

## Safety

The robot bridge publishes each sample once, and drops it while OCP reports a fault
(`stop_on_ocp_fault`) and when it may be older than `max_age_ms` (default 500, OCP's
own latency limit). The age bound is OCP's latency, the round trip of a token from
the Streamer's clock back to it, plus how long the sample waited on the
workstation: two durations, each measured on one machine. When the operator's data
stops, nothing more is published, so robot-side timeouts work as they do on a local
network. OCP raises a fault within half a second of losing the operator bridge, the
Player or the link.

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

To carry other topics, mount a config of your own (`-v $PWD/my.yaml:/my.yaml`,
`-p config:=/my.yaml`).

## Limits

- Commands cross once per Player frame, and OCP skips a frame that comes less than
  10 ms after the last one: 30 times a second for a headless Player by default, about
  75 with `use_idle_framerate_cap : true; idle_framerate_cap : 90.0;` in the project.
  A cap of 100 Hz halves the rate.
- The Streamer's OCP reads at most 4 KiB per control message; `budget_bytes` keeps
  the user data under it.
- Strings travel in the schema, so a topic whose strings change on every message
  resends its schema every time.

## Measured

2026-10-27, office LAN, Jetson AGX Thor robot (Oden Streamer nightly 0.26.1027)
and an RTX 4070 workstation (Oden Player), teleop_ros2 replaying its MCAP fixture
with this directory's config, video crossing the same link. Delay is from the
teleop_ros2 publish to delivery on the robot. Uplink is everything the workstation
sent the robot, counted by the firewall: Oden alone sends about 135 kbit/s with the
Player at 90 Hz and 50 kbit/s at 30 Hz.

| Case | Delay p50 / p95 / max | Uplink |
|---|---|---|
| controller_teleop, Player 90 Hz | 6–8 / 13–16 / 25 ms | 243 kbit/s |
| hand_teleop, Player 90 Hz | 7–8 / 19–21 / 26 ms | 256 kbit/s |
| full_body, Player 90 Hz | 7–8 / 15–16 / 23 ms | 217 kbit/s |
| hand_teleop plus `/xr_teleop/hand` and `/tf` | 7–12 / 19–24 / 32 ms | 305 kbit/s |
| controller_teleop, Player 30 Hz | 12–13 / 26–27 / 32 ms | 91 kbit/s |
| hand_teleop, Player 30 Hz | 16–18 / 34–36 / 47 ms | 98 kbit/s |
| hand_teleop, 25 ms added each way, 1 % loss | 32–34 / 44–46 / 54 ms | 253 kbit/s |
| vehicle_teleop ZMQ commands to the Panda worker | 5 / 9 / 22 ms | about 30 kbit/s of user data |

Every value delivered on the robot equalled the workstation's on the 0.0001 grid,
and every stamp was unchanged (`/tf` crosses merged per frame). The fixture moves
little, so most of its values do not change between frames; tracking data where
most values change every frame costs up to about twice the user data. Stopping
the operator bridge or killing the Player stopped delivery at once and raised an
OCP fault within 0.5 s; delivery resumed 1.0 s after the Player restarted.
