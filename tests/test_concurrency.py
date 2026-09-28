import os
import sys
import random
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Support running from root or tests dir
sys.path.insert(0, str(Path(__file__).parent.parent))
from tests.client import AuctionClient


async def run_concurrency_stress_test():
    print("=== STARTING CONCURRENCY & RACE CONDITION TEST ===")
    client = AuctionClient()

    # 1. Create auction
    end_time = datetime.now(timezone.utc) + timedelta(minutes=2)
    status, auction, _ = client.create_auction(
        creator_id="user-1",
        description="Limited Edition GPU",
        base_price=1000.0,
        end_date=end_time.isoformat(),
    )
    assert status == 201
    auction_id = auction["id"]
    print(f"1. Created auction '{auction_id}' with base price $1000.00")

    # 2. Start two SSE listener clients in background
    sse_events_1 = []
    sse_events_2 = []
    stop_event = asyncio.Event()

    async def sse_worker(client_id, event_list):
        async for evt in client.connect_sse(auction_id):
            event_list.append(evt)
            if stop_event.is_set():
                break

    task1 = asyncio.create_task(sse_worker("client-1", sse_events_1))
    task2 = asyncio.create_task(sse_worker("client-2", sse_events_2))
    await asyncio.sleep(0.5)

    # 3. Fire 12 concurrent bids ($1050 to $1600)
    users = ["user-1", "user-2", "user-3"]
    bid_prices = [1050.0, 1100.0, 1250.0, 1150.0, 1300.0, 1200.0, 1400.0, 1350.0, 1500.0, 1450.0, 1600.0, 1550.0]
    expected_winning_price = max(bid_prices)  # 1600.0

    print(f"2. Firing {len(bid_prices)} concurrent bids concurrently (Max target: ${expected_winning_price})...")

    async def submit_bid(user, price):
        loop = asyncio.get_running_loop()
        st, res, _ = await loop.run_in_executor(None, client.place_bid, auction_id, user, price)
        return st, res

    tasks = [submit_bid(random.choice(users), price) for price in bid_prices]
    results = await asyncio.gather(*tasks)

    for st, res in results:
        assert st == 202
        assert res["status"] == "IN_PROCESS"
    print(f"   All {len(results)} bids accepted by Gateway and produced to Kafka.")

    # 4. Wait for Kafka workers to process all bids
    print("3. Waiting for Kafka worker pool (3 replicas) to complete processing...")
    await asyncio.sleep(3.0)

    # 5. Check auction final state in database
    _, final_auction, _ = client.get_auction(auction_id)
    final_price = float(final_auction["max_price"])
    print(f"4. Final auction max_price in DB: ${final_price} (Expected: ${expected_winning_price})")
    assert final_price == expected_winning_price, f"Expected {expected_winning_price}, got {final_price}"

    # 6. Verify bid history
    _, bids, _ = client.get_bids(auction_id)
    print(f"5. Total successful winning bids in history: {len(bids)}")
    assert len(bids) > 0

    # 7. Verify both SSE clients received identical events
    stop_event.set()
    task1.cancel()
    task2.cancel()

    accepted_1 = [e for e in sse_events_1 if e.get("type") == "bid_accepted"]
    accepted_2 = [e for e in sse_events_2 if e.get("type") == "bid_accepted"]
    print(f"6. SSE Client 1 received {len(accepted_1)} bid updates")
    print(f"   SSE Client 2 received {len(accepted_2)} bid updates")
    assert len(accepted_1) > 0
    assert len(accepted_1) == len(accepted_2), f"Mismatched event counts: {len(accepted_1)} vs {len(accepted_2)}"
    assert accepted_1 == accepted_2, "Clients received different event streams!"

    print("\n CONCURRENCY TEST PASSED WITH ZERO DATA CORRUPTION & IDENTICAL SSE BROADCASTS! ")


if __name__ == "__main__":
    asyncio.run(run_concurrency_stress_test())
