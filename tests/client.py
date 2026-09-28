import os
import json
import asyncio
import urllib.request
import urllib.error
import urllib.parse
from typing import AsyncGenerator, Dict, Any, Tuple


class AuctionClient:
    """
    Lightweight, dependency-free Python client for the Online Auction System.
    Uses only Python standard library (urllib + asyncio).
    """

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or os.getenv("API_URL", "http://localhost:8080")).rstrip("/")

    def _request(self, method: str, path: str, data: Dict[str, Any] | None = None) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        body = None
        if data is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(data).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                content = resp.read().decode("utf-8")
                parsed_body = json.loads(content) if content else {}
                return resp.status, parsed_body, resp_headers
        except urllib.error.HTTPError as e:
            err_headers = {k.lower(): v for k, v in e.headers.items()}
            err_content = e.read().decode("utf-8")
            try:
                err_body = json.loads(err_content)
            except Exception:
                err_body = {"detail": err_content}
            return e.code, err_body, err_headers

    def create_auction(
        self,
        creator_id: str,
        description: str,
        base_price: float,
        end_date: str,
        images: list[str] | None = None,
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        return self._request(
            "POST",
            "/auctions",
            {
                "creator_id": creator_id,
                "description": description,
                "base_price": base_price,
                "end_date": end_date,
                "images": images or [],
            },
        )

    def get_auction(self, auction_id: str) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        return self._request("GET", f"/auctions/{auction_id}")

    def get_bids(self, auction_id: str) -> Tuple[int, list[Dict[str, Any]], Dict[str, str]]:
        return self._request("GET", f"/auctions/{auction_id}/bids")

    def place_bid(self, auction_id: str, placer_id: str, price: float) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        return self._request(
            "POST",
            f"/auctions/{auction_id}/bids",
            {"placer_id": placer_id, "price": price},
        )

    async def connect_sse(self, auction_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Asynchronously connects to the Server-Sent Events (SSE) stream for an auction.
        Yields parsed event payloads as they arrive.
        """
        parsed = urllib.parse.urlparse(f"{self.base_url}/auctions/{auction_id}/stream")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        reader, writer = await asyncio.open_connection(parsed.hostname, port)

        request_str = (
            f"GET {parsed.path} HTTP/1.1\r\n"
            f"Host: {parsed.netloc}\r\n"
            f"Accept: text/event-stream\r\n"
            f"Connection: keep-alive\r\n\r\n"
        )
        writer.write(request_str.encode("utf-8"))
        await writer.drain()

        # Read HTTP headers until empty line
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                text = line.decode("utf-8").strip()
                if text.startswith("data: "):
                    yield json.loads(text[6:])
        finally:
            writer.close()
            await writer.wait_closed()
