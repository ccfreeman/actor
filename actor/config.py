import logging
from dataclasses import dataclass, field

import aiohttp


logging.basicConfig(level=logging.DEBUG)


@dataclass
class Config:
    """Configuration for the app"""


CONFIG = Config()
