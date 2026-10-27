# Deploy

All GitHub Actions jobs run on a self-hosted runner on the production server
(novsrv), registered for this repository under `~/actions-runner` as the
`deploy` user (systemd unit `actions-runner@Smart-schedule-IRNITU.service`).

CI (`.github/workflows/cd-deploy.yml`) on every push to `master`:

1. lint (in a `python:3.8` container) + `docker compose config` check;
2. `rsync` the checkout into the deploy directory (keeping `.env` and `vpn/config/` intact);
3. `docker compose build && docker compose up -d --remove-orphans` there.

Images are built on the server — no registry involved. PR checks
(`checkers.yml`) build images under the separate compose project
`smart-schedule-ci` and remove them afterwards, so they never touch the live stack.

The deploy directory defaults to `/home/deploy/Smart-schedule-IRNITU`; override it
with the `DEPLOY_PATH` repository variable. No SSH secrets are needed any more.

## One-time server setup (files kept out of git)

```
<DEPLOY_PATH>/.env                 # cp .env.example .env, fill in tokens/creds
<DEPLOY_PATH>/vpn/config/awg0.conf  # AmneziaWG client config, see ../vpn/README.md
```

`CALENDAR_PUBLIC_BASE_URL` and `CALENDAR_SERVICE_PORT` in `.env` must line up with
the reverse proxy. `calendar_service` publishes on `127.0.0.1:8091` by default.

## Reverse proxy

`nginx.schedule.conf` — host nginx vhost that exposes `calendar_service`
(`127.0.0.1:8091`) as `schedule.nikitagricanuk.ru` for calendar subscriptions.
Copy to `/etc/nginx/conf.d/`, put the origin cert in
`/etc/cloudflare/certs/nikitagricanuk.ru/`, then `nginx -t && systemctl reload nginx`.
On SELinux hosts run `restorecon` on the copied file.
