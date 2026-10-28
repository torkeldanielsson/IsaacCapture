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
acknowledged, and its numbers. Every sample carries all of its numbers, so it
decodes with its schema alone, and a lost control message never affects a later
one. Floats cross as whole multiples of the topic's `precision` when it has one,
and a run of three or more zeros shrinks to two bytes. OCP stores all of it as
CBOR.

OCP keeps forwarding the last data a bridge wrote after the bridge stops. Each
control message therefore carries a count that moves on with every message the
bridge writes, and the other end acts only on messages whose count moved: a
stopped bridge's last data is never published. A bridge that has just started
waits one message.

Message stamps cross exactly as they were sent.

`/oden_teleop/status` (`diagnostic_msgs/DiagnosticArray`) on both ends reports the
OCP state, rates, drops, sample ages and, on the workstation, the bandwidth towards
the robot in total and per topic (`to_robot_kbit_s`, `to_robot_kbit_s_by_channel`);
`-p stats_file:=PATH` appends the same as JSON lines.

## Safety

Every check runs on the robot:

- Only topics listed in the robot's own copy of the config are published there.
- No command passes while OCP reports a fault (`stop_on_ocp_fault`). OCP's faults
  carry the health of both ends: lost input or feedback, a stopped camera, a slow
  Streamer, high latency.
- Every command is checked for age. OCP measures the loop from a time stamp the
  Streamer puts in the video to the operator's commands that return it. The robot
  bridge adds how long the sample waited at the workstation, and drops it when the
  total may exceed `max_age_ms` (default 500, OCP's own latency limit).
- Each topic keeps only its newest sample, and each sample is published once, so a
  stalled link never replays a backlog.
- Latched topics hold until they are replaced, so the age check leaves out how long
  ago they were published; one held back by a fault is offered again.
- When the operator's data stops, nothing more is published, so robot-side
  timeouts work as they do on a local network. OCP raises a fault within half a
  second of losing the operator bridge, the Player or the link.

The robot's own controllers keep their limits, interlocks and emergency stop.

## Running it

Both machines run Oden 0.26 or later with the OCP plugin (`libocp.so`) next to the
application. The `oden-vr` and `oden-fleet-streamer` packages ship it; with plain
`oden-streamer`, copy it from `oden-fleet-streamer` of the same version. The
Streamer starts with OCP on, the Player with OCP off: tick the *Oden Control
Pipeline* plugin in OdenVR, or set `disabled : false;` in
`/var/lib/oden/OdenVR/libocp_2de7c4c4-8cb6-48f1-a59d-bc3007d9df22.conf`.

Run OdenVR headless (`oden-vr --headless --project PROJECT`). A windowed OdenVR
follows the screen, and slows to about one frame a second when the screen blanks;
OCP then stops the commands.

Build the bridge image on each machine from the repository root. On a Jetson whose
kernel lacks iptables' `raw` table, add `--network=host`:

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

Start `teleop_ros2` with the same discovery setting, or its topics reach the robot
directly over the network, past the robot's checks.

### Without a headset

`teleop_ros2` replays recorded input. Isaac's reference image (about 12 GB) holds
a generator for a recording; the second argument is its length in frames, 60 a
second (36000 is ten minutes):

```bash
docker build -f examples/teleop_ros2/Dockerfile -t teleop_ros2_ref .
mkdir -p mcap
docker run --rm --entrypoint bash -v $PWD/mcap:/mcap teleop_ros2_ref -c \
  "source /usr/local/bin/teleop-env-setup && /opt/isaaccapture/install/examples/teleop_ros2/cpp/integration_tests/teleop_ros2_mcap_generator /mcap/fixture.mcap 36000"
docker run --rm --net=host --ipc=host -e ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST \
  -v $PWD/mcap:/mcap teleop_ros2_ref \
  --ros-args -p mode:=controller_teleop -p mcap_replay_path:=/mcap/fixture.mcap
```

On the robot, anything that publishes `/joint_states` stands in for the robot.

## Limits

- Commands cross once per Player frame, and OCP skips a frame that comes less than
  10 ms after the last one: 30 times a second for a headless Player by default,
  75–95 with `use_idle_framerate_cap : true; idle_framerate_cap : 90.0;` at the top
  level of the project's `config`.
  A cap of 100 Hz halves the rate.
- The Streamer's OCP reads at most 4 KiB per control message; `budget_bytes` keeps
  the user data under it.
- Strings travel in the schema, so a topic whose strings change on every message
  resends its schema every time.
- `to_operator` topics cross once per Streamer frame, so at the camera's frame rate.
- Without a `precision`, a float can differ from the sender's in its last bit.
- With several Oden links, every link carries the full control data and video.
- While the video stops, camera_viz shows the last frame; it picks the video up
  again about 2 s after it returns.

## Measured

2026-10-28, office LAN, Jetson AGX Thor robot (Oden Fleet Streamer nightly
0.26.1028) and an RTX 4070 workstation (Oden Player 0.26.1028), teleop_ros2
replaying its MCAP fixture with this directory's config, video crossing the same
link. Delay is from the teleop_ros2 publish to delivery on the robot. Uplink is
everything the workstation sent the robot, counted by the firewall: Oden alone
sends about 137 kbit/s with the Player at 90 Hz and 59 kbit/s at 30 Hz.

| Case | Delay p50 / p95 / max | Uplink |
|---|---|---|
| controller_teleop, Player 90 Hz | 7–8 / 15–16 / 25 ms | 341 kbit/s |
| hand_teleop, Player 90 Hz | 8 / 17–19 / 30 ms | 299 kbit/s |
| full_body, Player 90 Hz | 9 / 16–17 / 23 ms | 385 kbit/s |
| hand_teleop plus `/xr_teleop/hand` and `/tf` | 8–10 / 18–23 / 31 ms | 617 kbit/s |
| controller_teleop, Player 30 Hz | 15 / 28 / 34 ms | 125 kbit/s |
| hand_teleop, Player 30 Hz | 19–21 / 35–36 / 44 ms | 113 kbit/s |
| hand_teleop, 25 ms added each way, 1 % loss | 34 / 45–47 / 59 ms | 299 kbit/s |
| vehicle_teleop ZMQ commands to the Panda worker | 6 / 11 / 18 ms | about 38 kbit/s of user data |

With Jetson AGX Orin and Xavier NX robots, the median delay at 90 Hz was 13–19 ms.
Losing one of two links changed nothing.

Every value delivered on the robot equalled the workstation's on the 0.0001 grid,
and every stamp was unchanged (`/tf` crosses merged per frame). Every message
carries full values, so a topic costs the same whether its values move or not.
Stopping the operator bridge or killing the Player stopped delivery at once and
raised an OCP fault within 0.5 s; delivery resumed 1.3 s after the Player
restarted.
