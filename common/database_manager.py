"""
Database manager for MLP pipeline
"""

import logging
from abc import abstractmethod


class DatabaseManager:
    """Manager for SQLite database operations"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def setup(self):
        pass
