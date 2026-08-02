
# AI GENERATED

"""Runtime .proto compilation and dynamic module loading utilities."""

# Postpone evaluation of type annotations (PEP 563).
# This avoids evaluating annotations at import time which can trigger
# import-time side effects or import cycles when we dynamically load
# generated modules (useful for runtime proto compilation/loading).
from __future__ import annotations

import importlib
import importlib.util
import subprocess
import sys
import tempfile
import types
from pathlib import Path
from types import ModuleType
from typing import Optional, Tuple, Union


def compile_proto(proto_path: Union[str, Path], out_dir: Optional[Union[str, Path]] = None) -> Path:
    """
    Compile a .proto file using grpc_tools.protoc into `out_dir`.

    Returns the output directory Path containing generated files.
    """
    proto_path = Path(proto_path)
    if out_dir is None:
        out_dir = Path(tempfile.mkdtemp(prefix="user_proto_"))
    else:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto_path.parent}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        f"--pyi_out={out_dir}",
        str(proto_path),
    ]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"protoc failed: {e}") from e
    except FileNotFoundError as e:
        raise RuntimeError("grpc_tools.protoc not available. Install grpcio-tools.") from e

    return out_dir


def _load_module_from_file(module_name: str, file_path: Union[str, Path]) -> ModuleType:
    """Load a Python module from a file path without mutating sys.path."""
    file_path = Path(file_path)
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {file_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def register_modules(pb2: ModuleType, pb2_grpc: ModuleType, package: str = "interface") -> None:
    """
    Register generated modules into sys.modules under `package.message_pb2` and
    `package.message_pb2_grpc`, and expose them as attributes on the package module.

    This allows existing code that does `import pookiepy.message_pb2` to keep
    working without changing import sites.
    """
    pkg = sys.modules.get(package)
    if pkg is None:
        pkg = types.ModuleType(package)
        sys.modules[package] = pkg

    # set canonical names / packages
    pb2.__name__ = f"{package}.message_pb2"
    pb2.__package__ = package
    pb2_grpc.__name__ = f"{package}.message_pb2_grpc"
    pb2_grpc.__package__ = package

    setattr(pkg, "message_pb2", pb2)
    setattr(pkg, "message_pb2_grpc", pb2_grpc)

    sys.modules[f"{package}.message_pb2"] = pb2
    sys.modules[f"{package}.message_pb2_grpc"] = pb2_grpc


def load_pb_modules_from_dir(
    dir_path: Union[str, Path],
    package: str = "interface",
    register: bool = True,
) -> Tuple[ModuleType, ModuleType]:
    """
    Load `message_pb2.py` and `message_pb2_grpc.py` from a directory and optionally register them.
    Returns (pb2_module, pb2_grpc_module).
    """
    dir_path = Path(dir_path)
    pb2_file = dir_path / "message_pb2.py"
    pb2_grpc_file = dir_path / "message_pb2_grpc.py"
    if not pb2_file.exists() or not pb2_grpc_file.exists():
        raise FileNotFoundError(f"Generated files not found in {dir_path}")

    # use a temporary unique module name prefix to avoid collisions
    prefix = f"user_generated_{abs(hash(str(dir_path))) }"
    pb2 = _load_module_from_file(f"{prefix}.message_pb2", pb2_file)

    # Some generated *_pb2_grpc.py files import `message_pb2` as a top-level module
    # (no package). To make that import succeed when loading the generated grpc
    # module from a custom directory, temporarily register the loaded pb2 module
    # under the bare name 'message_pb2' in sys.modules, then remove it afterwards
    # so we don't pollute the global module namespace.
    sys.modules["message_pb2"] = pb2
    try:
        pb2_grpc = _load_module_from_file(f"{prefix}.message_pb2_grpc", pb2_grpc_file)
    finally:
        sys.modules.pop("message_pb2", None)

    validate_interface(pb2, pb2_grpc)

    if register:
        register_modules(pb2, pb2_grpc, package=package)

    return pb2, pb2_grpc


def compile_and_register(
    proto_path: Union[str, Path],
    package: str = "interface",
    out_dir: Optional[Union[str, Path]] = None,
) -> Tuple[ModuleType, ModuleType]:
    """
    Compile a .proto file and register the generated modules under the provided package name.

    Returns (pb2_module, pb2_grpc_module).
    """
    out = compile_proto(proto_path, out_dir=out_dir)
    pb2, pb2_grpc = load_pb_modules_from_dir(out, package=package, register=True)
    return pb2, pb2_grpc


def validate_interface(pb2: ModuleType, pb2_grpc: ModuleType) -> None:
    """
    Validate that the loaded modules contain the minimal expected symbols.
    Raises RuntimeError on missing symbols.
    """
    missing = []
    for name in ("Message", "ClientProvides", "ServerProvides"):
        if not hasattr(pb2, name):
            missing.append(f"pb2.{name}")

    if not hasattr(pb2_grpc, "StreamStub"):
        missing.append("pb2_grpc.StreamStub")
    if not hasattr(pb2_grpc, "StreamServicer"):
        missing.append("pb2_grpc.StreamServicer")

    if missing:
        raise RuntimeError(
            "Loaded proto modules are missing required symbols: " + ", ".join(missing)
        )


def resolve_modules(message_module: Optional[Union[str, ModuleType]] = None,
                    grpc_module: Optional[Union[str, ModuleType]] = None,
                    module_path: Optional[Union[str, Path]] = None,
                    package: str = "interface") -> Tuple[ModuleType, ModuleType]:
    """
    Resolve and return (message_pb2_module, message_pb2_grpc_module) from one of:
      - module objects passed directly
      - import strings (e.g. 'myproto.message_pb2')
      - a directory path containing generated files
      - fall back to packaged `interface` modules
    """
    # already module objects
    if isinstance(message_module, types.ModuleType) and isinstance(grpc_module, types.ModuleType):
        return message_module, grpc_module

    # import strings
    if isinstance(message_module, str) and isinstance(grpc_module, str):
        return importlib.import_module(message_module), importlib.import_module(grpc_module)

    # directory path containing generated files
    if module_path is not None:
        pb2, pb2_grpc = load_pb_modules_from_dir(module_path, package=package, register=False)
        validate_interface(pb2, pb2_grpc)
        return pb2, pb2_grpc

    # fall back to bundled interface
    try:
        import pookiepy.message_pb2 as default_pb2  # type: ignore  # pylint: disable=import-outside-toplevel
        import pookiepy.message_pb2_grpc as default_pb2_grpc  # type: ignore  # pylint: disable=import-outside-toplevel
        validate_interface(default_pb2, default_pb2_grpc)
        return default_pb2, default_pb2_grpc
    except Exception as e:  # pylint: disable=broad-exception-caught
        raise RuntimeError("Unable to resolve proto modules") from e
