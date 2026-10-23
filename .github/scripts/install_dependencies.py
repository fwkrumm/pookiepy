"""Install CI dependencies, or probe resolution without changing the environment."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def main() -> None:
    """Resolve the same requirements and binary-wheel policy for probes and installs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    command = [
        "uv", "pip", "install", "--python", sys.executable,
        "--only-binary", "grpcio,grpcio-tools",
    ]
    if args.dry_run:
        command.append("--dry-run")
    for requirement in os.environ.get("CI_REQUIREMENTS", "").splitlines():
        if requirement.strip():
            command.extend(["-r", requirement.strip()])
    command.extend(
        package.strip()
        for package in os.environ.get("CI_PACKAGES", "").splitlines()
        if package.strip()
    )
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()