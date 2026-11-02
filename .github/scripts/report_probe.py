"""Report non-blocking latest-Python probe outcomes consistently on all runners."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def main() -> None:
    """Emit a workflow warning and append an actionable job summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("status", choices=("skipped", "failed"))
    args = parser.parse_args()
    title = f"Latest Python probe {args.status}"
    details = (
        "Required dependencies or gRPC binary wheels unavailable; checks skipped."
        if args.status == "skipped"
        else "Check job logs for Python compatibility issues or missing dependency wheels."
    )
    message = (
        f"{os.environ.get('GITHUB_JOB', 'probe')} on "
        f"{os.environ.get('RUNNER_OS', 'local')}: {details} "
        "This probe is non-blocking; pinned Python checks remain mandatory."
    )
    print(f"::warning title={title}::{message}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as output:
            output.write(f"### {title}\n{message}\n")


if __name__ == "__main__":
    main()