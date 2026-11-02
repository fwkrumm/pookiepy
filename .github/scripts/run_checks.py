"""Run one CI check suite using the selected interpreter."""

from __future__ import annotations

import argparse
import subprocess
import sys


def run_module(*arguments: str) -> None:
    """Run a Python module and propagate failures to the CI job."""
    subprocess.run([sys.executable, "-m", *arguments], check=True)


def main() -> None:
    """Dispatch a named suite without duplicating test commands in workflow YAML."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "suite", choices=("unit", "examples", "integration", "unit-integration", "lint")
    )
    args = parser.parse_args()
    if args.suite in ("unit", "unit-integration"):
        run_module(
            "coverage", "run", "-m", "unittest", "discover",
            "-s", "tests", "-p", "test_*.py",
        )
    if args.suite == "examples":
        run_module("unittest", "discover", "-s", "tests/examples", "-p", "test_*.py")
    if args.suite in ("integration", "unit-integration"):
        subprocess.run(
            [sys.executable, "tests/integration/run_integration_tests.py"], check=True
        )
    if args.suite == "lint":
        run_module("pylint", "--jobs=1", "pookiepy", "tests", "examples")
    if args.suite in ("unit", "unit-integration"):
        run_module("coverage", "report", "-m")


if __name__ == "__main__":
    main()