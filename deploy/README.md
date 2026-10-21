# Deployment: systemd units for the EC2 host

These are copies of the units running on the EC2 instance. The Python
services run on the host from the repo's `.venv`, and Kafka, Schema Registry
and PostgreSQL run in Docker (`infra/docker-compose.yml`). The files in
`systemd/` are the source of truth: edit them here, then install them on the
server as shown below. Editing `/etc/systemd/system/` by hand means the next
rebuild silently loses the change.

| Unit | Runs | Listens on |
|---|---|---|
| `backend-api.service` | FastAPI (`uvicorn backend.app.main:app`) | 127.0.0.1:8000 |
| `dashboard.service` | Streamlit (`streamlit run app.py`, from `dashboard/`) | 127.0.0.1:8501 |
| `binance-producer.service` | Binance trades WebSocket → Kafka `crypto.trades` | none |
| `finnhub-producer.service` | Finnhub WebSocket → Kafka `stocks.quotes` | none |

All four run as `ec2-user`, restart 5 s after a crash, and are capped at
`MemoryMax=256M`. The instance has about 2 GB of RAM shared with Kafka and
PostgreSQL. With the cap, a leak in one service gets that service killed
and restarted, instead of pushing the whole box into swap.

Secrets are not in these files. Every service reads `.env` (and Docker reads
`infra/.env`) from the repo at runtime, so the units are safe to commit.

## Install or update

From the repo root on the server, after `git pull`:

```bash
sudo cp deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now backend-api dashboard binance-producer finnhub-producer
```

To apply a changed unit to a service that's already running, run
`sudo systemctl restart <name>` after the `daemon-reload`.

## Access model: localhost plus an SSH tunnel

Nothing here listens on a public interface. To use the dashboard, forward
the port over SSH and open http://localhost:8501:

```bash
ssh -i crypto-stock-platform-key.pem -N -L 8501:127.0.0.1:8501 ec2-user@<host>
```

The security group therefore only needs port 22, from your own IP.

## Gotcha: Streamlit's working directory

Streamlit reads `.streamlit/config.toml` only from the directory it is
started in. `dashboard.service` must keep
`WorkingDirectory=.../dashboard` and run `app.py`, not `dashboard/app.py`
from the repo root. If it runs from the root, the config is silently
ignored. The service then binds to `0.0.0.0` instead of `127.0.0.1`, loses
the theme, and sends usage statistics. A quick check:

```bash
sudo ss -ltnp | grep 8501   # must show 127.0.0.1:8501, not 0.0.0.0
```

## Not deployed as services

`streaming/consumers/bronze_writer.py` and the ETL steps (`pipelines/etl/`,
`warehouse/loader.py`) have no units yet. Airflow isn't installed on this
host, so the warehouse only updates when they are run by hand.
