"""Test package --- ensures the project root is on sys.path for all test modules."""
import sys
from pathlib import Path

# Ensure the project root is importable when tests are run as a package
# (python -m unittest tests.*) or discovered by a test runner.
sys.path.insert(0, str(Path(__file__).parent.parent))
