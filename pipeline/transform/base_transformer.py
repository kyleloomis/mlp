"""
Base transformer class for MLP pipeline
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from pipeline.config.run_configuration import RunConfiguration


class BaseTransformer(ABC):
    """Base transformer class for MLP pipeline"""

    def __init__(self, config: RunConfiguration):
        self.config = config
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform the input data"""
        pass
