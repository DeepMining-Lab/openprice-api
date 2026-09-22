# Deploying API V3

Status on this host (`openprice`, checked 2026-09-22): steps 1–4 below are done — the sync timer is
installed and enabled, and `openprice-api.service` was restarted and serves `/v3/*`. Only step 5
(the multi-worker/loopback-bind proposal) is not applied. Re-run this checklist after pulling new code
on another host, or if you ever reinstall this one.

1. Build the Parquet store once (~2 min, reads the CSVs, writes only under `~/openprice/parquet`):
   `.venv/bin/python -m app.v3.sync`
2. Keep it fresh: install `openprice-v3-sync.service` + `.timer` in `/etc/systemd/system/`, then
   `systemctl daemon-reload && systemctl enable --now openprice-v3-sync.timer`. Each run only reads the
   bytes appended since the previous one (a rewritten file is detected and rebuilt on its own).
3. Restart `openprice-api.service` once to load the V3 code (`systemctl restart openprice-api`).
4. Check: `curl localhost:8000/v3/ready`. The running API then picks up every later manifest update
   within `v3.manifest_poll_seconds`; no further restart is needed after step 3.
5. Not applied here: `deploy/openprice-api.service` is a *proposal* — several uvicorn workers and a
   loopback-only bind behind a reverse proxy (TLS, rate limiting). The live unit still binds
   `0.0.0.0:8000` with 1 worker, unchanged from before V3, because real traffic currently reaches it
   directly on that address with no reverse proxy in front; switching to loopback-only would cut that
   traffic until a proxy is in place. Apply it only as a deliberate, separate change.
