"""Generate protobuf bindings used by CI checks."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CUSTOM_INTERFACE_ROOT = Path("tests/integration/custom_interface")


def generate_bindings(proto_file: Path, include_root: Path, output_root: Path) -> None:
    """Compile one protobuf interface into Python, gRPC, and typing bindings."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{include_root}",
            f"--python_out={output_root}",
            f"--grpc_python_out={output_root}",
            f"--pyi_out={output_root}",
            str(proto_file),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def parse_args() -> argparse.Namespace:
    """Parse CI generation options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-custom",
        action="store_true",
        help="also generate the custom integration-test interface",
    )
    return parser.parse_args()


def main() -> None:
    """Generate bundled bindings and optionally custom test bindings."""
    args = parse_args()
    generate_bindings(Path("pookiepy/message.proto"), Path("."), Path("."))

    if args.include_custom:
        generate_bindings(
            CUSTOM_INTERFACE_ROOT / "custom_if/message.proto",
            CUSTOM_INTERFACE_ROOT,
            CUSTOM_INTERFACE_ROOT,
        )


if __name__ == "__main__":
    main()
