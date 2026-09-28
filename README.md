# Online Auction System (Microservices & Clean LLD Architecture)

A distributed, real-time Online Auction platform adhering to **SOLID principles**, **Domain-Driven Design (DDD)**, and **Ports & Adapters (Clean Architecture)**. Features an **API Gateway (Nginx)**, decoupled **Microservices**, **Apache Kafka** event streaming, **PostgreSQL** optimistic concurrency control, **Redis Pub/Sub** for Server-Sent Events (SSE), and a **Redis Sorted Set (ZSET)** delayed task scheduler for auction expiration.

---

## Architecture Diagram

```
                                  ┌────────────────┐
                                  │   Client / UI  │
                                  └───────┬────────┘
                                          │ Port 8080
                                          ▼
                             ┌─────────────────────────┐
                             │  API GATEWAY (Nginx)    │
                             └────────────┬────────────┘
               /auctions (CRUD)           │           /auctions/{id}/bids & stream
           ┌──────────────────────────────┴──────────────────────────────┐
           ▼                                                             ▼
┌──────────────────────┐                                    ┌────────────────────────┐
│   Auction Service    │                                    │ Bidding Service (x3)   │
│ (POST/GET /auctions) │                                    │ (Kafka Producer + SSE) │
└──────────┬───────────┘                                    └───────────┬────────────┘
           │                                                            │
           │                                                            ▼ (Key: auction_id)
           │                                                 ┌──────────────────────┐
           │                                                 │ Kafka: auction.bids  │
           │                                                 └──────────┬───────────┘
           │                                                            │
           │                                                            ▼
           │                                                 ┌──────────────────────┐
           │                                                 │ Kafka Workers (x3)   │
           │                                                 └──────────┬───────────┘
           │                                   Optimistic Lock (SQL)    │ Broadcast Outcome
           │                                  (WHERE max_price < bid)   │
           ▼                                                            ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Database & Redis Pub/Sub                       │
└───────────────────────────────────────▲───────────────────────────────────────┘
                                        │
                         ┌──────────────┴──────────────┐
                         │ Expiration Scheduler Worker │
                         │ (Redis ZSET delayed task)   │
                         └─────────────────────────────┘
```

---

## Clean Directory Structure

```
.
├── apps/                         # Executable Service Daemons & API Entrypoints
│   ├── auction_app.py            # FastAPI service for Auction CRUD
│   ├── bidding_app.py            # FastAPI service for Bid Ingestion & SSE Broadcast
│   ├── worker_app.py             # Consumer Worker for OCC Bid Processing
│   └── scheduler_app.py          # Background Worker for Auto-closing Expired Auctions
├── src/                          # Core Domain & Application Layers
│   ├── domain/                   # Enterprise Business Rules (DDD Entities & VOs)
│   │   ├── entities.py           # Rich Domain Models (Auction, Bid, Item)
│   │   ├── enums.py              # StrEnum status types (BidStatus, AuctionStatus)
│   │   └── value_objects.py      # Immutable Value Objects (BidResult)
│   ├── interfaces/               # Dependency Inversion Ports (Abstract Interfaces)
│   │   ├── repositories.py       # AuctionRepository, BidRepository
│   │   ├── publishers.py         # NotificationPublisher (Observer Pattern)
│   │   └── producers.py          # BidQueueProducer
│   ├── infrastructure/           # Adapters & External Drivers
│   │   ├── db.py                 # AsyncPG connection pool & transaction manager
│   │   ├── queries.py            # Centralized SQL statements (single-trip CTEs)
│   │   ├── postgres_repo.py      # Concrete PostgreSQL Repository with OCC
│   │   ├── redis_publisher.py    # Redis Pub/Sub implementation
│   │   └── kafka_producer.py     # AI येKafka Producer partitioned by auction_id
│   └── services/                 # Application Use Cases & Orchestration
│       ├── auction_service.py    # AuctionManagerService
│       └── bidding_service.py    # BiddingEngineService
├── docker/                       # Infrastructure & Deployment Assets
│   ├── Dockerfile.auction        # Container spec for Auction Service
│   ├── Dockerfile.bidding        # Container spec for Bidding Service
│   ├── Dockerfile.worker         # Container spec for Worker Pool
│   ├── Dockerfile.scheduler      # Container spec for Scheduler Worker
│   └── nginx.conf                # API Gateway reverse proxy & unbuffered SSE config
├── migrations/                   # Database Schemas & Data Seeding
│   ├── 01_schema.sql             # Relational schemas with constraints & indexes
│   └── 02_seed.sql               # Seed test users and default data
├── tests/                        # Automated Test Suites & Test Clients
│   ├── client.py                 # Zero-dependency Python Auction HTTP & SSE Client
│   ├── test_suite.py             # 5/5 Functional integration tests
│   └── test_concurrency.py       # 12-thread parallel OCC & synchronized SSE test
├── docker-compose.yml            # Complete multi-service orchestration
├── requirements.txt              # Pinned core dependencies
└── README.md
```

---

## Services & Replicas

| Service | Replicas | Role | Technology |
| :--- | :---: | :--- | :--- |
| **`gateway`** | 1 | Reverse proxy routing & unbuffered SSE proxying | Nginx Alpine (`:8080`) |
| **`auction_service`** | 1 | Auction creation & item metadata (Low traffic) | FastAPI, asyncpg (`:8000`) |
| **`bidding_service`** | **3** | High-throughput Kafka bid ingestion & SSE streaming | FastAPI, aiokafka, redis (`:8000`) |
| **`worker`** | **3** | Kafka consumer pool, optimistic DB locking & pub/sub | aiokafka, asyncpg, redis |
| **`scheduler`** | 1 | Polls Redis ZSET to auto-close expired auctions | redis.asyncio, asyncpg |
| **`postgres`** | 1 | Primary persistent database with migration scripts | PostgreSQL 16 Alpine (`:5434`) |
| **`redis`** | 1 | Pub/Sub event bus & Delayed task Sorted Set | Redis 7 Alpine (`:6381`) |
| **`kafka`** | 1 | Distributed event log (6 partitions, KRaft mode) | Apache Kafka 3.8.0 (`:9092`) |

---

## One-Command Startup & Testing

### 1. Build and Run Entire Platform
```bash
docker compose up -d --build
```

### 2. Verify Services
```bash
docker compose ps
```

### 3. Run Automated Tests
```bash
python tests/test_suite.py
python tests/test_concurrency.py
```
