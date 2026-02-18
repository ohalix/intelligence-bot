import abc
from datetime import datetime
from typing import Any, Dict, List
import aiohttp

class BaseIngester(abc.ABC):
    def __init__(self, config: Dict[str, Any], session: aiohttp.ClientSession):
        self.config = config
        self.session = session

    @abc.abstractmethod
    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        raise NotImplementedError
