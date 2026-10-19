# ADR: Docker resource limits for the local stack

- **Status:** Accepted
- **Date:** 2026-10-19
- **Scope:** `infra/docker-compose.yml` (platform) and `infra/airflow/docker-compose.yml` (Airflow)

## Context

The whole platform runs on one development laptop:

| Resource | Hardware | What Docker gets |
|---|---|---|
| CPU | Intel i7-5600U, 2 physical cores / 4 threads | 4 logical CPUs |
| RAM | 16 GB | ~7.7 GB (Docker Desktop's WSL 2 VM defaults to half of host RAM) |

Twelve long-running containers share that: ZooKeeper, Kafka, Kafka UI, Schema
Registry, the warehouse Postgres, and seven Airflow services (metadata
Postgres, Redis, API server, scheduler, DAG processor, Celery worker,
triggerer). Without limits, one runaway process (a DAG stuck in a loop, a JVM
growing its heap, a Kafka log compaction) can take the whole laptop with it.

## Decision

Every service gets a hard memory limit and a CPU cap
(`deploy.resources.limits`), and every JVM, Postgres and Airflow process is
tuned so its normal usage fits inside that limit.

### Final limits, with what the soak test observed

Observed numbers come from a 15-minute soak on 2026-10-19 with the full stack
running and the Binance producer streaming ~25 trades/s into Kafka.

| Service | Memory limit | CPU cap | Tuning inside the limit | Soak peak memory | Soak avg / peak CPU |
|---|---|---|---|---|---|
| zookeeper | 256 MB | 0.25 | heap 128 MB | 106 MB (41%) | 2% / 18% |
| kafka | 1 GB | **1.5** (was 1.0) | heap 512 MB; the rest is page cache | 570 MB (56%) | 128% / 210% |
| kafka-ui | 384 MB | 0.25 | heap 256 MB | 257 MB (67%) | 9% / 54% |
| schema-registry | 448 MB | 0.25 | heap 256 MB | 267 MB (60%) | 12% / 73% |
| warehouse-postgres | 512 MB | 0.5 | `shared_buffers=128MB`, `max_connections=50` | 35 MB (7%) | 6% / 40% |
| airflow-postgres | 384 MB | 0.5 | `max_connections=50` | 60 MB (16%) | 9% / 31% |
| airflow-redis | 128 MB | 0.1 | `maxmemory 64mb`, `noeviction` | 5 MB (4%) | 1% / 4% |
| airflow-apiserver | 768 MB | 0.5 | `AIRFLOW__API__WORKERS=1` | 220 MB (29%) | 1% / 3% |
| airflow-scheduler | 768 MB | 0.75 | | 301 MB (39%) | 12% / 21% |
| airflow-dag-processor | **1 GB** (was 512 MB) | 0.5 | `min_file_process_interval=60` | 512 MB (**100%, OOM-killed**) at the old limit; 346 MB after | 41% / 88% |
| airflow-worker | 1 GB | 1.0 | `WORKER_CONCURRENCY=2` | 656 MB (64%) | 84% / 154% |
| airflow-triggerer | **768 MB** (was 384 MB) | 0.25 | | 384 MB (**100%, OOM-killed**) at the old limit; 395 MB after | 32% / 50% |
| **Total** | **~7.3 GB of limits** | 6.35 caps on 4 threads | | **~3.2 GB actually used** | |

Bold values changed as a result of the soak; see "What the soak test changed".
`docker stats` on Docker Desktop reports CPU as a percentage of one CPU, and
it overshoots caps on short samples, so treat the CPU columns as relative load.

### Why these numbers

**Memory is the hard limit, CPU is the soft one.** A container that exceeds
its memory limit has a process killed by the kernel (`OOMKilled`). If the
whole VM runs out, the kernel kills *something*, often Kafka or Postgres,
which is far worse. So memory limits are sized from real usage plus headroom.
A container that hits its CPU cap just waits; nothing dies. That is why the CPU
caps add up to more than the 4 available threads: services are rarely all busy
at once, and the caps only exist so one runaway process can't freeze the
laptop.

**Limits are caps, not reservations.** The memory limits add up to ~7.3 GB,
close to the VM's 7.7 GB, but the stack actually uses ~3.2 GB. The limits say
how far each service may grow before *it* is killed, rather than the VM
picking a victim at random.

**JVM heap must be smaller than the container limit.** Kafka, ZooKeeper,
Schema Registry and Kafka UI are Java. A JVM uses memory outside the heap too
(metaspace, thread stacks, direct buffers, GC structures). Setting the heap
equal to the limit guarantees an OOM kill. Each heap is set to roughly half to
two thirds of its limit. Java services also sit at a flat memory level by
design, because the heap is allocated up front, so a high but *flat* number is
fine; a *climbing* one is a leak.

**Kafka gets most of its memory as page cache, not heap.** Kafka reads recent
messages from the OS page cache, so its 512 MB heap leaves ~500 MB of the
1 GB limit for cache. A bigger heap would make Kafka slower, not faster.

**The Airflow worker runs 2 tasks at a time, not the default 16.** Sixteen
parallel tasks on 2 physical cores just thrash: every task runs slower and the
total finishes later. Two keeps both cores busy without contention.

**One API server worker.** The default starts several ~150 MB copies of the
API server; one is plenty for a single user.

**Redis uses `noeviction`.** Redis is only the Celery message queue. The
default cache policy would silently drop queued tasks when full; `noeviction`
refuses new writes instead, which surfaces the problem.

## What the soak test changed

1. **Kafka CPU 1.0 → 1.5.** At 1.0 Kafka sat near its cap while producers ran,
   which slowed Schema Registry writes (it stores schemas in a Kafka topic) to
   ~40 s and will get worse once consumers read in parallel.
2. **DAG processor memory 512 MB → 1 GB.** It sat pinned at 512 MB for the
   whole soak and had processes OOM-killed. That breaks DAG parsing: new or
   edited DAGs would not show up in the UI.
3. **Triggerer memory 384 MB → 768 MB.** The Airflow runtime alone is about
   380 MB, so 384 MB left no room at all; it was OOM-killed too.
4. **Health check timeout 10 s → 30 s** for the DAG processor and triggerer.
   Their checks start a full Airflow CLI, which takes 10 to 20 s on a fraction
   of a CPU, so they were reported unhealthy even while working. After the
   change the DAG processor reports healthy (346 MB of 1 GB, no OOM). The
   triggerer no longer OOMs (395 MB of 768 MB, which would have been killed at
   the old 384 MB), but on its 0.25 CPU the check still takes over 30 s, so it
   shows `unhealthy` while running normally. Nothing depends on its health
   status yet; revisit (raise its CPU cap or switch to a lighter check) when
   the first deferrable sensor is added.

## Consequences

- **Memory is sustainable.** ~3.2 GB used by containers, and host RAM stayed
  flat at about 12.7 to 13.0 GB of 16 GB for the whole soak, with no growth.
- **CPU is the bottleneck.** With everything running, the host CPU sat at
  100% for most of the soak, dropping to 25 to 90% once the Airflow services
  settled. The stack works, but the laptop is sluggish while all of it runs.
  For daily work, run the platform project (Kafka and friends) all the time
  and start the Airflow project only when working on DAGs. They are separate
  compose projects for exactly this reason:

  ```bash
  cd infra
  docker compose up -d --wait                                            # platform
  docker compose --env-file .env -f airflow/docker-compose.yml up -d     # Airflow, when needed
  docker compose --env-file .env -f airflow/docker-compose.yml stop      # Airflow off again
  ```

- The total of the memory limits (~7.3 GB) is close to the VM size. That is
  safe because limits are caps, but there is little room to raise any limit
  further without giving the VM more memory (see below).

## When the hardware changes

- **Give Docker more memory first.** Create `%UserProfile%\.wslconfig`:

  ```ini
  [wsl2]
  memory=10GB
  processors=4
  swap=4GB
  ```

  then `wsl --shutdown` and restart Docker Desktop.
- **32 GB of RAM:** raise the Airflow worker to 2 GB and
  `WORKER_CONCURRENCY` to 4, and Kafka to 2 GB (heap 1 GB).
- **More cores:** raise the Kafka and worker CPU caps first; they are the two
  services that do real work. Everything else is mostly idle.

## How this maps to AWS

In production nobody sets container limits by hand like this; you pick an
instance size per managed service: Amazon MSK for Kafka (broker instance
type), RDS for the warehouse and Airflow metadata Postgres, and MWAA for
Airflow (environment class and worker count). The soak numbers above give a
first estimate for those sizes, for example that the scheduler and API server
need well under 1 GB while the worker is the component to scale.
