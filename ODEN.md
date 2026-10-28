<!--
SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
SPDX-License-Identifier: MIT
-->

# Isaac Teleop over Voysys Oden

This fork of [Isaac Capture](https://github.com/NVIDIA/IsaacCapture) runs Isaac
Teleop against a robot on another network, over Wi-Fi, 4G/5G or the internet. One
[Voysys Oden](https://voysys.se) link carries the robot's cameras to the operator,
the operator's commands to the robot, and the robot's state back.

![One Oden link carries the robot's cameras to the operator, the operator's commands to the robot, and the robot's state back](docs/oden/01-overview.png)

- **The robot's cameras in camera_viz.** A camera_viz source, `type: oden`, reads
  each camera straight from OdenVR's GPU memory, with no second encode or decode.
- **Any ROS 2 topic or ZMQ stream, both ways.** Two small bridges,
  [teleop_oden](examples/teleop_oden/README.md), carry Isaac's teleop topics to the
  robot and its joint states back, through Oden Control Pipeline (OCP).
- **Every command is checked on the robot** (see [Safety](#safety)).

Oden needs nothing beyond its nightly build. The additions in this fork are MIT
licensed; the rest of Isaac Capture keeps its own licences.

## Getting started

Oden needs a licence on each machine. Voysys Free is free for non-commercial use,
with a renewable 7-day trial for commercial use:
[sign up](https://portal.voysys.dev/signup).

Install the Oden Fleet Streamer (the Streamer with the OCP plugin) on the robot, a
Jetson or an Ubuntu PC, and OdenVR on the workstation, Ubuntu with an NVIDIA GPU:

```bash
# robot
curl -fsSL https://releases.voysys.dev/install.sh | sh -s -- fleet-streamer --install-service false --nightly latest
oden-streamer --activate YOUR-STREAMER-KEY
# workstation
curl -fsSL https://releases.voysys.dev/install.sh | sh -s -- vr --nightly latest
oden-vr --activate YOUR-ODENVR-KEY
```

Stream the robot's cameras to OdenVR as in Oden's
[first vehicle stream](https://docs.voysys.dev/start/first-vehicle-stream) guide,
and enable the OCP plugin at both ends. In OdenVR, add a **CUDA IPC Texture
Export** for each camera, with Share Path `/tmp/oden_0`. Then show it in
camera_viz:

```bash
git clone -b oden https://github.com/torkeldanielsson/IsaacCapture.git && cd IsaacCapture
examples/camera_viz/camera_viz.sh setup
examples/camera_viz/camera_viz.sh run examples/camera_viz/configs/oden.yaml --mode window
```

![camera_viz showing a test camera from an Oden Streamer, read from OdenVR's GPU texture export](docs/oden/07-camera-viz-window.png)

For commands, start the bridges from the same checkout, on both machines:

```bash
docker build -f examples/teleop_oden/Dockerfile -t teleop_oden .
# on the robot; on the workstation, the same with operator_bridge
docker run --rm --net=host --ipc=host -e ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST \
  teleop_oden python3 -m isaaccapture_examples.teleop_oden.robot_bridge \
  --ros-args -p config:=/opt/teleop_oden/config/teleop_ros2.yaml
```

Run Isaac's `teleop_ros2` as usual. Its topics appear on the robot, and the
robot's `/joint_states` appear on the workstation. One YAML file lists what
crosses, so your own topics can go too:

```yaml
to_robot:
  - topic: /xr_teleop/ee_poses
    precision: 0.0001        # floats rounded to 0.1 mm
  - topic: /my_robot/gripper_cmd
to_operator:
  - topic: /joint_states
```

The [teleop_oden README](examples/teleop_oden/README.md) covers ZMQ, headsets and
every setting.

## Safety

![Every command is checked on the robot: each message stands on its own, commands are checked for age, faults stop commands, and the robot's config decides which topics get through](docs/oden/03-command-path.png)

**The operator sees the present.** When the network gets worse, Oden lowers the
picture quality and keeps the video current.

**Every command is checked against the video it was based on.** The Streamer puts
an authenticated time stamp in the video, and OdenVR returns it with the commands
the operator gives while watching. The robot knows how old the operator's view was
and drops any command that may be older than 500 ms.

**Both ends know each other's health.** Every control message carries the health
of the whole link. While any fault is active, the robot passes no commands.

**Every message stands on its own.** Each message carries the full current command,
so a lost message has no effect on later ones.

**The robot has the last word.** Only topics in the robot's own config reach it, and
its controllers keep their own limits and emergency stop.

## Measured

A Jetson AGX Thor robot and an RTX 4070 workstation on an office network, Oden
nightly 0.26.1028 at both ends, and `teleop_ros2` replaying Isaac's recorded input:

![Command delay from Isaac to the robot: medians of 7–9 ms at 90 Hz](docs/oden/06-delay.png)

- Commands reach the robot 7–9 ms after Isaac publishes them (median, OdenVR at
  90 Hz).
- In controller mode, Isaac's topics add about 170 kbit/s to the uplink at 90 Hz.
- With 25 ms added each way and 1 % packet loss, delay rose by the 25 ms and
  nothing else changed.
- Stopping the operator side stopped commands within 10 ms, and OCP raised a fault
  within half a second.

Tested so far: camera_viz in a window, and `teleop_ros2` replaying recorded input.
Not yet tested: full XR sessions with a headset, and links over 5G.

## Links

- [teleop_oden README](examples/teleop_oden/README.md) and the
  [camera_viz Oden source](docs/source/references/camera_streaming.rst)
- [Isaac Capture](https://github.com/NVIDIA/IsaacCapture)
- [Oden documentation](https://docs.voysys.dev) and
  [nightlies](https://releases.voysys.dev/nightly/), including OdenVR for macOS
