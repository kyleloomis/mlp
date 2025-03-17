import logging
from abc import abstractmethod


class BaseOCRReader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    @abstractmethod
    def read(self, pdf_path: str):
        pass
