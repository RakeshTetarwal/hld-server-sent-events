import os
import sys
import time
import asyncio
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Support running from root or tests dir
sys.path.insert(0, str(Path(__file__).parent.parent))
from tests.client import AuctionClient


class TestAuctionAPI(unittest.TestCase):
    """Unit and functional client tests for REST endpoints and validation."""

    @classmethod
    def setUpClass(cls):
        cls.client = AuctionClient()

    def test_01_create_auction_success(self):
        end_time = datetime.now(timezone.utc) + timedelta(hours=2)
        status, body, headers = self.client.create_auction(
            creator_id="user-1",
            description="Sony PlayStation 5 Pro",
            base_price=699.99,
            end_date=end_time.isoformat(),
            images=["https://example.com/ps5.jpg"],
        )
        self.assertEqual(status, 201)
        self.assertIn("id", body)
        self.assertEqual(body["base_price"], 699.99)
        self.assertEqual(body["max_price"], 699.99)
        self.assertEqual(body["status"], "ACTIVE")
        self.assertEqual(body["item"]["description"], "Sony PlayStation 5 Pro")
        self.assertIn("application/json", headers.get("content-type", ""))

    def test_02_get_nonexistent_auction_returns_404(self):
        status, body, _ = self.client.get_auction("non-existent-auction-id")
        self.assertEqual(status, 404)
        self.assertIn("detail", body)

    def test_03_invalid_bid_price_rejected_synchronously(self):
        status, body, _ = self.client.place_bid("any-id", "user-2", -10.0)
        self.assertEqual(status, 400)
        self.assertIn("greater than zero", body.get("detail", ""))


class TestAuctionAsyncClient(unittest.IsolatedAsyncioTestCase):
    """Asynchronous client tests testing real-time SSE streaming, Kafka, and optimistic locking."""

    @classmethod
    def setUpClass(cls):
        cls.client = AuctionClient()

    async def test_04_async_bid_rejection_over_sse(self):
        end_time = datetime.now(timezone.utc) + timedelta(hours=1)
        _, auction, _ = self.client.create_auction(
            creator_id="user-1",
            description="Vintage Watch",
            base_price=100.0,
            end_date=end_time.isoformat(),
        )
        auction_id = auction["id"]

        events = []
        stop = asyncio.Event()

        async def listen():
            async for evt in self.client.connect_sse(auction_id):
                events.append(evt)
                if stop.is_set():
                    break

        task = asyncio.create_task(listen())
        await asyncio.sleep(0.5)

        # Bid of 50.0 is < base price 100.0 -> rejected by Worker over SSE
        status, res, _ = self.client.place_bid(auction_id, "user-2", 50.0)
        self.assertEqual(status, 202)
        tracking_id = res["tracking_id"]

        await asyncio.sleep(1.5)
        stop.set()
        task.cancel()

        rejected_events = [e for e in events if e.get("type") == "bid_rejected"]
        self.assertTrue(len(rejected_events) >= 1)
        self.assertEqual(rejected_events[0]["tracking_id"], tracking_id)
        self.assertEqual(rejected_events[0]["status"], "BID_TOO_LOW")

    async def test_05_concurrent_bids_and_sse_broadcast(self):
        end_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        status, auction, _ = self.client.create_auction(
            creator_id="user-1",
            description="Rare Pokemon Card",
            base_price=50.0,
            end_date=end_time.isoformat(),
        )
        self.assertEqual(status, 201)
        auction_id = auction["id"]

        events_received = []

        async def listen():
            try:
                async for event in self.client.connect_sse(auction_id):
                    events_received.append(event)
            except asyncio.CancelledError:
                pass

        listener_task = asyncio.create_task(listen())
        await asyncio.sleep(0.5)

        bids = [
            ("user-2", 60.0),
            ("user-3", 75.0),
            ("user-2", 90.0),
        ]

        for placer, price in bids:
            st, res, _ = self.client.place_bid(auction_id, placer, price)
            self.assertEqual(st, 202)
            self.assertEqual(res["status"], "IN_PROCESS")
            self.assertIn("tracking_id", res)

        await asyncio.sleep(2.0)

        # Verify final state in DB
        _, final_auction, _ = self.client.get_auction(auction_id)
        self.assertEqual(float(final_auction["max_price"]), 90.0)

        # Verify bid history count
        _, history, _ = self.client.get_bids(auction_id)
        self.assertEqual(len(history), 3)

        listener_task.cancel()
        await asyncio.gather(listener_task, return_exceptions=True)

        event_types = [e.get("type") for e in events_received]
        self.assertIn("snapshot", event_types)
        self.assertIn("bid_accepted", event_types)

        accepted_prices = [e["price"] for e in events_received if e.get("type") == "bid_accepted"]
        self.assertEqual(accepted_prices, [60.0, 75.0, 90.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
