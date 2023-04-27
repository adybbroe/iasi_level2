#!/usr/bin/env python3
"""Common definitions."""
import logging
from importlib.metadata import version

PACKAGE_NAME = __name__
__version__ = version(__name__)
LOG = logging.getLogger(__name__)
