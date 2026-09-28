from abc import ABC, abstractmethod
from typing import Optional
from src.domain.value_objects import BidResult


class NotificationPublisher(ABC):
    @abstractmethod
    async def publish_bid_outcome(self, result: BidResult) -> None:
        pass

    @abstractmethod
    async def publish_auction_closed(
        self, auction_id: str, winning_bid_id: Optional[str], final_price: float
    ) -> None:
        pass
