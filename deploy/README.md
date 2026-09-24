# Deploying API V3

Status on this host (`openprice`, checked 2026-09-22): steps 1–4 below are done — the sync timer is
installed and enabled, and `openprice-api.service` was restarted and serves `/v3/*`. Only step 5
(the multi-worker/loopback-bind proposal) is not applied. Re-run this checklist after pulling new code
on another host, or if you ever reinstall this one. The validity fixes of 2026-09-24 add steps 6 and 7.

1. Build the Parquet store once (a few minutes, reads the CSVs, writes only under `~/openprice/parquet`):
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
6. Chainlink phase switches: the sync reads them from an Ethereum node that serves historical state. Put
   `OPENPRICE_RPC_URL=<node URL>` in `/home/debian/.openprice_rpc_env` (`chmod 600`; the URL can carry an access
   token and is never written to the repository or to the store), then reinstall `openprice-v3-sync.service`
   (it now reads that file) and `systemctl daemon-reload`. Without the file the phase table is kept as it is and
   every run logs a warning; responses that depend on a phase switch not yet checked carry
   `chainlink_phase_unverified`.
7. Weekly full check: install `openprice-v3-verify.service` + `.timer`, then
   `systemctl enable --now openprice-v3-verify.timer`. It runs the sync with `--verify` (a full SHA-256 of the CSV
   bytes already converted, about 31 GB read once a week) and waits for a running sync instead of failing.

What a sync did, and why, is in `~/openprice/parquet/sync_log.jsonl` (one JSON line per rebuild, append and phase
switch, with the resulting dataset version); `GET /v3/datasets` shows the per-file counters.
