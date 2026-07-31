"""PDM build hook: generate protobuf Python bindings from message.proto.

Runs automatically during ``pip install .`` or ``python -m build``.
``grpcio-tools`` is available because it is listed in ``[build-system] requires``.
"""

from __future__ import annotations

from pathlib import Path


def pdm_build_initialize(context) -> None:  # noqa: ANN001
    """Generate message_pb2.py, message_pb2_grpc.py, and message_pb2.pyi.

    Args:
        context: PDM BuildContext (unused; generation is always performed).
    """
    from grpc_tools import protoc  # pylint: disable=import-outside-toplevel

    project_root = Path(__file__).parent
    proto_file = project_root / "pookiepy" / "message.proto"

    # Locate the google/*.proto well-known types bundled with grpcio-tools.
    try:
        import importlib.resources as _res  # pylint: disable=import-outside-toplevel

        grpc_proto_include = str(_res.files("grpc_tools").joinpath("_proto"))
    except AttributeError:
        # Fallback for Python < 3.9 (importlib.resources.files not available)
        import pkg_resources  # pylint: disable=import-outside-toplevel

        grpc_proto_include = pkg_resources.resource_filename("grpc_tools", "_proto")

    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{project_root}",
            f"-I{grpc_proto_include}",
            f"--python_out={project_root}",
            f"--grpc_python_out={project_root}",
            f"--pyi_out={project_root}",
            str(proto_file),
        ]
    )

    if result != 0:
        raise RuntimeError(
            f"protoc generation failed (exit {result}) for {proto_file}"
        )
