import sys
import uvicorn
import asyncio


def main():
    service = sys.argv[1] if len(sys.argv) > 1 else "auction-api"
    if service == "auction-api":
        uvicorn.run("apps.auction_app:app", host="0.0.0.0", port=8001, reload=True)
    elif service == "bidding-api":
        uvicorn.run("apps.bidding_app:app", host="0.0.0.0", port=8002, reload=True)
    elif service == "worker":
        from apps.worker_app import main as run_worker
        asyncio.run(run_worker())
    elif service == "scheduler":
        from apps.scheduler_app import main as run_scheduler
        asyncio.run(run_scheduler())
    else:
        print(f"Unknown service '{service}'. Choose from: auction-api, bidding-api, worker, scheduler")


if __name__ == "__main__":
    main()
