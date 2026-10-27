# syntax=docker/dockerfile:1
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# The Oden bridge for either end: ROS 2, the teleop_ros2 message package and this
# example; no isaaccapture, so it also suits the robot. From the repository root:
#   docker build -f examples/teleop_oden/Dockerfile -t teleop_oden .

ARG ROS_DISTRO=jazzy
FROM ros:${ROS_DISTRO}-ros-base
ARG ROS_DISTRO

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-cbor2 \
    python3-colcon-common-extensions \
    python3-msgpack \
    python3-pytest \
    python3-yaml \
    python3-zmq \
    ros-${ROS_DISTRO}-rosidl-default-generators \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/teleop_oden
COPY examples/teleop_ros2/teleop_ros2_interfaces src/teleop_ros2_interfaces
RUN bash -c "source /opt/ros/${ROS_DISTRO}/setup.bash \
    && colcon build --merge-install --base-paths src --install-base /opt/teleop_oden/install" \
    && rm -rf build log

COPY examples/teleop_oden/python python
COPY examples/teleop_oden/config config
COPY tests/python/examples/teleop_oden tests
ENV PYTHONPATH=/opt/teleop_oden/python

COPY --chmod=755 <<'EOF' /usr/local/bin/teleop-oden-entrypoint
#!/bin/bash
set -e
source "/opt/ros/${ROS_DISTRO}/setup.bash"
source /opt/teleop_oden/install/setup.bash
exec "$@"
EOF

ENTRYPOINT ["/usr/local/bin/teleop-oden-entrypoint"]
CMD ["python3", "-m", "isaaccapture_examples.teleop_oden.robot_bridge", \
     "--ros-args", "-p", "config:=/opt/teleop_oden/config/teleop_ros2.yaml"]
