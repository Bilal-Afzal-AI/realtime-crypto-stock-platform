# Local infrastructure

Two Docker Compose projects that together run everything the platform needs
on a laptop.

| Project | File | Services |
|---|---|---|
| `market-platform` | `infra/docker-compose.yml` | ZooKeeper + Kafka (Confluent 7.9.2), Schema Registry, Kafka UI, PostgreSQL 16 (warehouse + backend DBs), one-shot topic creator |
| `market-airflow` | `infra/airflow/docker-compose.yml` | Airflow 3.0.2 API server, scheduler, DAG processor, Celery worker, triggerer, metadata Postgres, Redis (Celery broker), optional Flower |

```
             market-data-net (shared, created by market-platform)
   ┌───────────────────────────────────────────────────────────────────┐
   │  zookeeper  kafka:29092  schema-registry:8081  kafka-ui  postgres  │
   │                                                                   │
   │                        airflow-worker   airflow-triggerer          │
   └───────────────────────────────┬───────────────────────────────────┘
                                   │ (only these two join both networks)
   ┌───────────────────────────────┴───────────────────────────────────┐
   │  airflow-apiserver  scheduler  dag-processor  airflow-postgres     │
   │  airflow-redis                         airflow-internal (private)  │
   └───────────────────────────────────────────────────────────────────┘
```

## Why two compose projects

They have different lifecycles. You rebuild and restart Airflow constantly
while writing DAGs; the broker and warehouse should stay up through that.
Separate projects mean `docker compose down` on Airflow can never stop Kafka or
delete its network (Airflow only *joins* `market-data-net` as `external`).
It also mirrors production, where Kafka, the warehouse and Airflow are three
separately managed services.

## Ports (all bound to 127.0.0.1)

| Service | From your laptop | From containers on `market-data-net` |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| Schema Registry | http://localhost:8081 | http://schema-registry:8081 |
| Kafka UI | http://localhost:8085 | |
| Postgres (`market` warehouse, `backend` app DB) | `localhost:5433` | `postgres:5432` |
| Airflow UI / API | http://localhost:8080 | |
| Flower (optional) | http://localhost:5555 | |

Postgres uses host port **5433** because a natively installed Postgres often
owns 5432. Every host port can be changed in `infra/.env`.

## Start the stack

Docker Desktop needs about 6 GB of memory for everything (Settings →
Resources). Run from the `infra/` directory:

```bash
cp .env.example .env          # then fill in passwords and generate the keys
docker compose up -d --wait   # platform first: creates market-data-net
docker compose --env-file .env -f airflow/docker-compose.yml up -d --build --wait
```

`--wait` blocks until every service's health check passes (or fails), so a
green exit means healthy, not merely "started". The first Airflow run builds
the image and migrates the metadata DB, which takes a few minutes.

## Verify each service

```bash
# Everything at a glance: look for (healthy) on each long-running service,
# and "Exited (0)" on the one-shot kafka-init and airflow-init jobs.
docker compose ps -a
docker compose --env-file .env -f airflow/docker-compose.yml ps -a

# Kafka: the broker answers and the project topics exist
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Kafka round trip: produce one message, read it back
docker exec kafka bash -c "echo hello | kafka-console-producer --bootstrap-server kafka:29092 --topic crypto.trades"
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic crypto.trades --from-beginning --max-messages 1 --timeout-ms 10000

# Schema Registry: returns [] until a producer registers a schema
curl http://localhost:8081/subjects

# Kafka UI
curl http://localhost:8085/actuator/health

# ZooKeeper: replies imok
docker exec zookeeper bash -c "echo ruok | nc localhost 2181"

# Postgres: both databases exist and accept connections
docker exec warehouse-postgres pg_isready -U market -d market
docker exec warehouse-postgres psql -U market -d market -c "\l"

# Airflow API server, scheduler, and the Celery worker
curl http://localhost:8080/api/v2/version
docker exec airflow-scheduler curl -s http://localhost:8974/health
docker compose --env-file .env -f airflow/docker-compose.yml exec airflow-worker airflow info

# Airflow worker can reach the platform over the shared network
docker compose --env-file .env -f airflow/docker-compose.yml exec airflow-worker \
  python -c "import socket; [socket.create_connection((h, p), 3) for h, p in [('kafka', 29092), ('postgres', 5432), ('schema-registry', 8081)]]; print('platform reachable')"
```

PowerShell users: replace `curl` with `curl.exe` (in Windows PowerShell 5,
`curl` is an alias for `Invoke-WebRequest`).

## Stop, and reset

```bash
docker compose --env-file .env -f airflow/docker-compose.yml down   # Airflow first
docker compose down                                                 # then the platform
```

`down` keeps the named volumes, so topics, messages, warehouse tables and DAG
run history survive. `down -v` deletes the volumes: a full factory reset.

---

## Why each service is configured the way it is

**ZooKeeper.** In classic mode Kafka keeps its cluster metadata (live brokers,
controller election, topic configs, partition assignments) in ZooKeeper, so the
broker waits for ZooKeeper's `ruok`/`imok` health check before starting. It
gets its own volumes because losing them while Kafka keeps its data leaves the
two disagreeing about the cluster, and the broker refuses to start. Note that
Confluent 7.9 is the last release line with ZooKeeper: Kafka 4.0 / Confluent
8.0 run only in KRaft mode, where Kafka manages its own metadata. Moving later
is a compose change (drop `zookeeper`, add controller settings to the broker);
producer and consumer code is unaffected.

**Kafka (single broker).** One broker is only acceptable locally. Two listeners exist
because containers and your laptop reach the broker by different names
(`kafka:29092` vs `localhost:9092`), and Kafka tells clients which address to
use after the first connection; advertise the wrong one and clients connect,
then fail. Internal topic replication factors are set to 1 because the default
of 3 can't be satisfied by one broker, which would silently break consumer
groups. Auto topic creation is off so a typo in a producer can't invent a new
topic; `kafka-init` creates `crypto.trades` and `stocks.quotes` (3 partitions
each, so up to 3 consumers in a group can share the load).

**Schema Registry.** The contract between producers and consumers: it rejects a
schema change that would break existing readers. It stores schemas in a
compacted Kafka topic, so it is stateless and needs no volume of its own.

**Kafka UI.** A browser view of topics, messages, consumer lag and schemas.
`DYNAMIC_CONFIG_ENABLED=false` keeps config in this file instead of UI edits
that disappear when the container is recreated. `provectuslabs/kafka-ui` is
pinned to its last release, v0.7.2; the maintained fork is `kafbat/kafka-ui`.

**PostgreSQL 16 (warehouse + backend).** One server with two databases:
`market`, the local stand-in for Snowflake/Redshift, and `backend`, the FastAPI
app's own data, owned by a separate `backend` role created by
`postgres/init/01-create-backend-db.sh`. Separate roles mean an API bug can't
touch analytics tables. In production these become two separate services
(a warehouse and an OLTP database) because their workloads differ completely.
The init script runs only when the volume is first created; to re-run it,
`docker compose down -v`. Credentials come from `.env` with `${VAR:?message}`
so compose refuses to start without them rather than creating a passwordless
database.

**Airflow with CeleryExecutor.** The scheduler only decides what to run and
queues it in Redis; separate worker containers execute tasks. That isolates
scheduling from heavy task code and lets you scale with
`--scale airflow-worker=3`. Airflow 3 components: the API server (UI, REST API,
and the Execution API workers report to), the scheduler, the DAG processor
(parsing split out so a broken DAG file can't stall scheduling) and the
triggerer (runs deferrable sensors without holding a worker slot).

**Separate Airflow metadata DB.** The scheduler makes constant small writes;
analytics runs large scans. Keeping them apart means neither slows down or
breaks the other, and you can reset one without touching the other.

**Redis, broker only.** It holds queued-but-not-started task messages. The
metadata DB is the source of truth for task state, so if Redis restarts the
scheduler re-queues the work. That's why Redis has no volume and no
published port.

**Custom Airflow image.** Providers (Kafka, Postgres) are installed at build
time against the official constraints file, not via `_PIP_ADDITIONAL_REQUIREMENTS`
at container start, which is slow, needs internet on every restart, and can
resolve different versions on different days.

**Networks.** Only `airflow-worker` and `airflow-triggerer` join
`market-data-net`, because only task code needs Kafka and the warehouse.
The UI and scheduler have no route to them: least privilege applied to the
network.

**Pinned images, 127.0.0.1 ports, explicit heaps.** Pinned tags make every
machine run what you tested. Binding to localhost stops Docker from exposing
services to your LAN (Docker's port publishing can bypass the host firewall).
JVM heaps are capped so the whole stack fits on a laptop.

## What breaks in production without health checks and volumes

**Without health checks**, `depends_on` only waits for a container to *start*,
not to be *ready*. Schema Registry starts before the broker accepts requests
and crash-loops; Airflow components start against a database that hasn't
finished migrating; a scheduler whose loop has frozen still shows as "Up".
Orchestrators (ECS, Kubernetes, load balancers) use the same checks to decide
whether to route traffic or restart a container. Without them, a hung process
keeps receiving work and nobody gets paged.

**Without persistent volumes**, the data lives in the container's writable
layer and disappears on `docker compose down`, an image upgrade, or a host
reboot that recreates containers:

- Kafka loses every message, and ZooKeeper loses the topic metadata. Consumers' committed
  offsets now point at data that doesn't exist, and any events not yet written
  to the lake are gone for good (no replay).
- The warehouse loses all tables.
- Airflow's metadata DB loses DAG run history, task states, and stored
  connections; the scheduler may re-run backfills it thinks never happened.

## How this maps to AWS

| Local | AWS managed service | What changes |
|---|---|---|
| ZooKeeper + Kafka (1 broker) | **Amazon MSK** (or MSK Serverless) | You never see ZooKeeper (MSK manages it, and new clusters use KRaft). Brokers spread over 3 AZs, replication factor 3 and `min.insync.replicas=2`, TLS and IAM/SASL auth instead of PLAINTEXT, AWS handles patching and broker replacement. You pay per broker-hour and storage. |
| Schema Registry | **AWS Glue Schema Registry** (or Confluent's on EC2/EKS) | Glue SR is serverless and IAM-secured, but uses its own serializer libraries, not the Confluent wire format. Switching later means changing producer/consumer serdes, so decide early. |
| Kafka UI | MSK console + CloudWatch metrics | Consumer lag and broker health come from CloudWatch/Prometheus; teams often still run Kafka UI in a private subnet. |
| Postgres (warehouse + backend) | **Redshift** (or Snowflake) for the warehouse; **RDS / Aurora PostgreSQL** for the backend DB | Multi-AZ failover, automated backups and point-in-time recovery, encryption at rest. These replace the Docker volume. |
| Airflow (Celery, 7 containers) | **Amazon MWAA** | AWS runs the scheduler, workers, web server, metadata DB and queue for you. You upload DAGs and a `requirements.txt` to S3; no Redis or metadata Postgres to manage, and workers autoscale. Less control over versions and plugins. |
| Redis (Celery broker) | **Amazon ElastiCache** (if self-hosting Airflow on ECS/EKS) | Not needed with MWAA, which uses SQS internally. |
| `infra/.env` | **AWS Secrets Manager** / SSM Parameter Store | Secrets fetched at runtime by IAM role, rotated automatically, every access audited. Airflow reads connections directly from it via a secrets backend instead of `AIRFLOW_CONN_*` variables. |
| `market-data-net` | VPC, private subnets, security groups | Security groups play the role of the shared network: only the MWAA workers' group may reach MSK and RDS. |
| Health checks | ECS/EKS health checks, ALB target groups, CloudWatch alarms | Same idea, plus automatic replacement of failing tasks and alerts. |
