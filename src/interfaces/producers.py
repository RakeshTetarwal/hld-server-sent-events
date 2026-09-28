from abc import ABC, abstractmethod


class IBidQueueProducer(ABC):
    @abstractmethod
    async def enqueue_bid(
        self, tracking_id: str, auction_id: str, placer_id: str, price: float
    ) -> None:
        pass
