"""
Base source class
"""

import logging
from abc import ABC


class BaseSource(ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
