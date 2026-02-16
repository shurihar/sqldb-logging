"""
Conftest module for pytest fixtures.
"""

import logging
import random

import pytest


@pytest.fixture
def buffer_size():
    """Returns a random buffer size in range [1, 10] including both end points."""
    return random.randint(1, 10)


@pytest.fixture
def flush_level():
    """Returns a random logging level."""
    return random.choice([logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG])
