# Deploy

CI (`.github/workflows/cd-deploy.yml`) on every push to `master`:

1. lint + `docker compose config` check;
2. `rsync` the repo to the server (keeping `.env` and `vpn/config/` intact);
3. `docker compose build && docker compose up -d --remove-orphans` on the server.

Images are built on the server — no registry involved.

## Required GitHub Actions secrets

| secret | value |
| --- | --- |
| `DEPLOY_HOST` | server hostname / IP |
| `DEPLOY_USER` | ssh user (in the `docker` group) |
| `DEPLOY_SSH_KEY` | private key; its public key must be in `~<user>/.ssh/authorized_keys` |
| `DEPLOY_PORT` | optional, defaults to `22` |
| `DEPLOY_PATH` | optional, defaults to `Smart-schedule-IRNITU` (relative to the user's home) |

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
