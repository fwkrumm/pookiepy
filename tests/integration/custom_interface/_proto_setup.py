"""Proto compile/validate side-effect module for custom_interface tests.

Import this module (via sys.path trick shown below) to:
  - add the project root to sys.path
  - compile the custom proto from custom_if/message.proto
  - validate that the compiled module replaced the internal packaged one

Usage in a sibling script:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))  # find this module
    import _proto_setup                             # runs setup as side-effect
    project_root = _proto_setup.PROJECT_ROOT
"""

import importlib
import sys
from pathlib import Path

from pookiepy.custom_interface import compile_and_register
from pookiepy.logger import get_logger as _get_logger

_THIS_DIR = Path(__file__).parent
PROJECT_ROOT = _THIS_DIR.parent.parent.parent  # root directory

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_logger = _get_logger("proto_setup")

_p = _THIS_DIR / "custom_if" / "message.proto"
if not _p.exists():
    raise RuntimeError(f"Expected proto file at {_p} but it does not exist")

_logger.info("Compiling and registering proto from %s", _p)
compile_and_register(_p, package="pookiepy", out_dir=_p.parent)

_imported_pb2 = importlib.import_module("pookiepy.message_pb2")
_internal_path = (PROJECT_ROOT / "pookiepy" / "message_pb2.py").resolve()
_imported_path = Path(_imported_pb2.__file__).resolve()

if _imported_path == _internal_path:
    raise RuntimeError(
        "Loaded pookiepy.message_pb2 is the internal packaged module; "
        "expected the external/custom interface"
    )

_logger.info("Using interface module from: %s", _imported_path)


def ensure_loaded():
    """No-op: called after importing to confirm side-effect ran (silences W0611)."""
