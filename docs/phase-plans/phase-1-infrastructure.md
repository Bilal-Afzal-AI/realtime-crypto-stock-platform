# Phase 1: Local infrastructure execution plan

**Status:** in progress, one step at a time with approval after each.

| Step | State |
|---|---|
| 1. Resource limits | Done |
| 2. ZooKeeper, Kafka, Kafka UI | Done and verified |
| 3. PostgreSQL | Done and verified (warehouse and backend databases) |
| 4. Schema Registry | Done and verified |
| 5. Airflow and Redis | Done. UI loads, scheduler logs clean. The triggerer health check is slow on 0.25 CPU and reports unhealthy while running. |
| 6. Soak test | Done (15 minutes, not the 6 hours originally planned). Found and fixed DAG processor and triggerer OOM kills. |
| 7. Document limits and why | Done, see `docs/architecture-decision-records/docker-resource-limits.md` |
**Goal:** run ZooKeeper, Kafka, Kafka UI, Schema Registry, PostgreSQL, Airflow
and Redis locally in Docker Compose, within the limits of a laptop, and prove
the stack is stable before building pipelines on top of it.

## The machine and the budget

| Resource | Hardware | What Docker actually gets |
|---|---|---|
| CPU | Intel i7-5600U: **2 physical cores, 4 threads**, 2.6 GHz (3.2 GHz turbo) | 4 logical CPUs (Docker Desktop reports `NCPU=4`) |
| RAM | 16 GB | **~7.7 GB** today. Docker Desktop's WSL 2 VM defaults to 50% of host RAM |

Two facts drive every choice in this plan:

1. **Memory is the hard limit.** When a container exceeds its memory limit the
   kernel kills it (`OOMKilled`). When the whole VM runs out, it kills
   something at random, often Kafka or Postgres. Windows itself, a browser and
   an IDE need roughly 5 to 6 GB, so the containers should stay around
   **6.5 GB in total**.
2. **CPU is the soft limit.** Two real cores is low for 12 containers. CPU
   limits are *caps*, not reservations: a container waits when it hits its cap
   but nothing gets killed. So the caps below add up to more than 4. That's
   deliberate, because the services are rarely busy at the same moment. The
   caps exist so that one runaway process (a DAG stuck in a loop, a Kafka log
   compaction) can't freeze the laptop.

### Current state (as of writing)

- `infra/docker-compose.yml`, `infra/airflow/docker-compose.yml`,
  `infra/.env.example` and `infra/README.md` are written but **not committed**.
- Some containers from the earlier attempt still exist: `kafka` and
  `warehouse-postgres` are running and healthy; `kafka-ui`, `schema-registry`
  and `kafka-init` were created but never started. The Schema Registry 7.9.2
  image never finished downloading.
- The Airflow image build was interrupted while it was still downloading the
  base image. It needs a retry.
- Old unused images (`cp-kafka:8.0.0`, `cp-schema-registry:8.0.0`) are taking
  about 2 GB of disk. You can remove them in Step 1.

Step 1 starts from a clean slate with `docker compose down`, which keeps the
data volumes.

### Optional, your decision: give Docker more memory

I recommend raising the WSL 2 VM to 10 GB. It is a setting on your machine,
so you make the change yourself. Create `%UserProfile%\.wslconfig` with:

```ini
[wsl2]
memory=10GB
processors=4
swap=4GB
```

Then run `wsl --shutdown` and restart Docker Desktop. The plan works without
this change, but the full stack will have very little headroom at 7.7 GB.

---

## Step 1: Add CPU and memory limits to every service

**What changes.** Each service gets a `deploy.resources.limits` block, and the
JVM, Postgres and Airflow get tuning so their memory use fits inside their
limit. A memory limit without tuning just means the process gets killed
instead of slowing down. Two more fixes so the services can come up in stages:

- `kafka-ui` stops depending on `schema-registry`. Otherwise, starting
  Kafka UI in stage one would also start Schema Registry. Kafka UI simply shows
  the registry as unavailable until Step 4.
- The Airflow worker runs 2 tasks at a time, not the default 16. Sixteen
  parallel tasks on 2 cores would just thrash.

| Service | Memory limit | CPU cap | Tuning inside the limit |
|---|---|---|---|
| zookeeper | 256 MB | 0.25 | heap 128 MB |
| kafka | 1 GB | 1.00 | heap 512 MB (the rest is OS page cache, which Kafka depends on for fast reads) |
| kafka-ui | 384 MB | 0.25 | heap 256 MB |
| schema-registry | 448 MB | 0.25 | heap 256 MB |
| warehouse-postgres | 512 MB | 0.50 | `shared_buffers=128MB`, `max_connections=50` |
| airflow-postgres | 384 MB | 0.50 | `max_connections=50` |
| airflow-redis | 128 MB | 0.10 | `maxmemory 64mb` |
| airflow-apiserver | 768 MB | 0.50 | `AIRFLOW__API__WORKERS=1` (the default starts several copies) |
| airflow-scheduler | 768 MB | 0.75 | |
| airflow-dag-processor | 512 MB | 0.50 | `min_file_process_interval=60` (re-parse DAGs every 60 s, not every 30 s) |
| airflow-worker | 1 GB | 1.00 | `WORKER_CONCURRENCY=2` |
| airflow-triggerer | 384 MB | 0.25 | |
| **Total** | **~6.4 GB** | 5.85 caps on 4 threads | |

Step 6 checks these numbers against real usage and Step 7 records the final
values.

**Commands** (from `infra/`):

```bash
docker compose down
docker compose --env-file .env -f airflow/docker-compose.yml down
```

Then list each service's limits (PowerShell):

```powershell
foreach ($f in @(@(), @('--env-file','.env','-f','airflow/docker-compose.yml'))) {
  $j = docker compose @f config --format json | ConvertFrom-Json
  "exit=$LASTEXITCODE"
  $j.services.PSObject.Properties | ForEach-Object { "{0,-22} cpus={1,-5} memory={2}" -f $_.Name, $_.Value.deploy.resources.limits.cpus, $_.Value.deploy.resources.limits.memory }
}
```

Optional: `docker image rm confluentinc/cp-kafka:8.0.0 confluentinc/cp-schema-registry:8.0.0` frees about 2 GB of disk.

**Success looks like:**
- Both `config` commands exit without errors, and every service shows a `memory` and a `cpus` value.
- Docker Desktop → Containers shows no running containers from either project.

---

## Step 2: Stage one, ZooKeeper, Kafka and Kafka UI

**Commands:**

```bash
docker compose up -d --wait zookeeper kafka kafka-init kafka-ui
docker compose ps -a
docker exec zookeeper bash -c "echo ruok | nc localhost 2181"
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker exec kafka bash -c "echo hello | kafka-console-producer --bootstrap-server kafka:29092 --topic crypto.trades"
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic crypto.trades --from-beginning --max-messages 1 --timeout-ms 10000
docker stats --no-stream zookeeper kafka kafka-ui
```

**Success looks like:**
- `up --wait` returns with exit code 0. With `--wait`, a non-zero exit means a health check failed.
- `ps -a` shows `zookeeper`, `kafka` and `kafka-ui` as `Up ... (healthy)`, and `kafka-init` as `Exited (0)`.
- ZooKeeper answers `imok`.
- The topic list contains `crypto.trades` and `stocks.quotes`.
- The consumer prints `hello` and `Processed a total of 1 messages`.
- http://localhost:8085 opens Kafka UI. The `local` cluster shows **1 broker, online**, and both topics have 3 partitions.
- **Docker Desktop:** under Containers → `market-platform`, the three containers have a green running dot. In each container's **Stats** tab, memory sits below its limit (Kafka about 400 to 700 MB of 1 GB).
- `docker stats` shows a `MEM USAGE / LIMIT` column that matches the table in Step 1 (for example `.../1GiB`). If it shows the whole VM's memory instead, the limits did not apply.

---

## Step 3: Add PostgreSQL and verify connectivity

**Commands:**

```bash
docker compose up -d --wait postgres
docker compose ps postgres
docker exec warehouse-postgres pg_isready -U market -d market
docker exec warehouse-postgres psql -U market -d market -c "\l"
docker exec -e PGPASSWORD=<BACKEND_DB_PASSWORD from .env> warehouse-postgres psql -h localhost -U backend -d backend -c "select current_user, current_database();"
docker run --rm --network market-data-net postgres:16.6 pg_isready -h postgres -p 5432
```

From the laptop, using the port mapping:

```bash
psql "postgresql://market@localhost:5433/market" -c "select version();"
```

(This one needs `psql` installed on Windows. Otherwise connect with DBeaver or pgAdmin to `localhost:5433`.)

**Success looks like:**
- `warehouse-postgres` shows `Up ... (healthy)`.
- `pg_isready` prints `accepting connections`.
- `\l` lists both the `market` and `backend` databases, with `backend` owned by `backend`.
- The backend query returns `backend | backend`, which proves the separate role works.
- The throwaway container on `market-data-net` prints `postgres:5432 - accepting connections`. That is the same network path Airflow will use.
- From the laptop, `localhost:5433` connects. Port 5432 belongs to your native Postgres and is untouched.
- **Docker Desktop:** `warehouse-postgres` has a green dot. Its Stats tab shows roughly 40 to 150 MB of the 512 MB limit while idle.

---

## Step 4: Add Schema Registry and verify it's reachable

**Commands:**

```bash
docker compose pull schema-registry
docker compose up -d --wait schema-registry
docker compose ps schema-registry
curl.exe http://localhost:8081/subjects
curl.exe -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": \"{\\\"type\\\":\\\"string\\\"}\"}" http://localhost:8081/subjects/smoke-test-value/versions
curl.exe http://localhost:8081/subjects
curl.exe -X DELETE http://localhost:8081/subjects/smoke-test-value
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic _schemas
```

**Success looks like:**
- The pull completes. This image is the one whose download failed earlier.
- `schema-registry` shows `Up ... (healthy)`.
- The first `GET /subjects` returns `[]`.
- The POST returns `{"id":1}`, and the second GET returns `["smoke-test-value"]`.
- The `_schemas` topic exists with `cleanup.policy=compact`, which shows the registry stores its schemas in Kafka.
- In Kafka UI (http://localhost:8085), the **Schema Registry** menu now loads instead of showing an error.
- **Docker Desktop:** green dot. Memory is around 250 to 400 MB of the 448 MB limit. Java services sit near their limit by design, because the heap is pre-allocated.

---

## Step 5: Add Airflow and Redis last, and verify the UI and scheduler

Airflow goes last because it is the heaviest part of the stack (7 containers)
and depends on everything else.

**Commands** (the build downloads about 350 MB the first time):

```bash
docker compose --env-file .env -f airflow/docker-compose.yml build
docker compose --env-file .env -f airflow/docker-compose.yml up -d --wait
docker compose --env-file .env -f airflow/docker-compose.yml ps -a
curl.exe http://localhost:8080/api/v2/version
docker exec airflow-scheduler curl -s http://localhost:8974/health
docker exec airflow-redis redis-cli ping
docker compose --env-file .env -f airflow/docker-compose.yml exec airflow-worker celery --app airflow.providers.celery.executors.celery_executor.app inspect ping
docker compose --env-file .env -f airflow/docker-compose.yml exec airflow-worker python -c "import socket; [socket.create_connection((h, p), 3) for h, p in [('kafka', 29092), ('postgres', 5432), ('schema-registry', 8081)]]; print('platform reachable')"
```

Then an end-to-end check with a throwaway DAG. I'll add
`orchestration/dags/smoke_test.py`, which has one task that runs
`SELECT 1` against the `market_warehouse` connection. Trigger it in the UI,
and delete the file once the check passes.

**Success looks like:**
- The build ends with `naming to docker.io/library/market-airflow:3.0.2 done`.
- `airflow-init` shows `Exited (0)`. Every other Airflow container shows `Up ... (healthy)`.
- `/api/v2/version` returns JSON containing `"version":"3.0.2"`.
- The scheduler health check returns `"status":"healthy"` for both `metadatabase` and `scheduler`.
- Redis returns `PONG`, and Celery `inspect ping` returns `pong` from one worker.
- The worker prints `platform reachable`.
- http://localhost:8080 shows the Airflow login. You can sign in with `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` from `infra/.env`.
- In the UI, the `smoke_test` DAG appears within about a minute (that proves the DAG processor works). A manual run turns **green / success** (that proves the scheduler, Celery, worker and warehouse connection all work).
- **Docker Desktop:** the `market-airflow` group shows 7 green dots and 1 exited `airflow-init`. Total memory across both projects is under about 6.5 GB.

---

## Step 6: Soak test, full stack under light load

**Why.** "Healthy after 2 minutes" isn't the same as "stable for a day".
Memory leaks, log growth, JVM heap creep and CPU thermal throttling only show
up over time. On a 2-core laptop it's better to find out now than in the
middle of building pipelines.

**Duration:** 6 hours, or overnight if you prefer. The laptop should be on
mains power with sleep turned off (Windows Settings → System → Power).

**Load:** a steady 50 messages per second into `crypto.trades`, which is
about what one live crypto feed produces, plus one consumer reading it:

```bash
docker exec -d kafka kafka-producer-perf-test --topic crypto.trades --num-records 1080000 --record-size 300 --throughput 50 --producer-props bootstrap.servers=kafka:29092
docker exec -d kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic crypto.trades --group soak-test > NUL
```

**Recording:** sample every 5 minutes into a CSV file (PowerShell, from the repo root):

```powershell
New-Item -ItemType Directory -Force docs/phase-plans/soak | Out-Null
1..72 | ForEach-Object {
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" |
    ForEach-Object { "$(Get-Date -Format s),$_" } |
    Add-Content docs/phase-plans/soak/stats.csv
  Start-Sleep -Seconds 300
}
```

**End-of-test checks:**

```bash
docker inspect -f "{{.Name}} oom={{.State.OOMKilled}} restarts={{.RestartCount}}" $(docker ps -aq)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group soak-test
docker system df -v
```

**Success looks like:**
- Every container reports `oom=false restarts=0`.
- No container's peak `MemPerc` in `stats.csv` goes above **85%** of its limit, except the Java services, which hold a fixed heap. For those, the number must stay flat rather than climbing.
- Memory is flat across the last 3 hours, with less than 10% growth for any container. Steady growth means a leak or an undersized limit.
- Average total CPU stays below about 60% of 4 threads while idle plus load, so the laptop stays usable.
- Consumer-group `LAG` for `soak-test` is near 0, which means the broker keeps up.
- `docker system df -v` shows the `kafka-data` volume grew by roughly the expected amount (about 330 MB of messages over 6 hours) and the Airflow logs are not growing out of control.
- **Docker Desktop:** every container still has a green dot. The Containers view header shows total CPU and memory use, and memory should look flat rather than rising. In **Windows Task Manager**, the `VmmemWSL` process should stay roughly constant.

If anything fails, I adjust that service's limit or tuning and repeat only
the part of the soak needed to confirm the fix.

---

## Step 7: Document the resource limits and why

Update `infra/README.md` with a **Resource limits** section containing:

- The final table: limit, cap, tuning, and the **observed peak from the soak** per service, so each number has evidence behind it.
- The reasoning: memory is the hard limit and CPU the soft one, JVM heap versus container limit (the heap must be smaller, because the JVM also needs memory outside the heap), and why worker concurrency is 2.
- What to change when the hardware changes (for example: 32 GB of RAM → raise the worker to 2 GB and concurrency to 4), and the `.wslconfig` recommendation.
- How this compares with AWS: on MSK, RDS and MWAA you choose an instance size per service instead of setting container limits, and the soak data gives a first estimate for those sizes.

Then commit the infra files, the plan, and the README update in one commit.
The soak CSV stays out of git because `*.csv` is already ignored.

**Commands:**

```bash
git add infra docs/phase-plans README.md .env.example
git status
git commit -m "Add local infrastructure stack with resource limits"
```

**Success looks like:**
- `git status` shows **no** `infra/.env` (it is ignored) and no soak CSV.
- The commit lands on `main`. It is not pushed until you say so.
