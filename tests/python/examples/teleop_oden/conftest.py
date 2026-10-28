# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: Apache-2.0

# Resolve `isaaccapture_examples.teleop_oden` against the in-tree source. In the
# example's container (examples/teleop_oden/Dockerfile) repo_paths.py is absent
# and PYTHONPATH already points at the example.

import sys
from pathlib import Path

_tests_python = Path(__file__).resolve().parents[2]

if (_tests_python / "repo_paths.py").is_file():
    if str(_tests_python) not in sys.path:
        sys.path.insert(0, str(_tests_python))

    from repo_paths import repo_root  # noqa: E402

    # python/ is the root of the PEP 420 namespace isaaccapture_examples.
    sys.path.insert(0, str(repo_root() / "examples" / "teleop_oden" / "python"))
