from src.domain.value_objects import BidResult
from src.interfaces.repositories import AuctionRepository
from src.interfaces.publishers import NotificationPublisher


class BiddingEngineService:
    """Application Service orchestrating optimistic locking and notifications."""

    def __init__(
        self,
        auction_repo: AuctionRepository,
        notifier: NotificationPublisher,
    ):
        self.auction_repo = auction_repo
        self.notifier = notifier

    async def process_bid(
        self, tracking_id: str, auction_id: str, placer_id: str, price: float
    ) -> BidResult:
        success, status, updated_auction, bid_id = await self.auction_repo.update_highest_bid_optimistic(
            auction_id=auction_id, placer_id=placer_id, price=price
        )

        result = BidResult(
            tracking_id=tracking_id,
            auction_id=auction_id,
            placer_id=placer_id,
            price=price,
            success=success,
            status=status,
            updated_auction=updated_auction,
            bid_id=bid_id,
        )

        # Broadcast outcome (accepted or rejected) to all listening clients
        await self.notifier.publish_bid_outcome(result)
        return result
