#!/bin/bash
# Bring up the AmneziaWG tunnel (userspace amneziawg-go) and keep the container
# alive as the network namespace holder for tg_bot.
set -euo pipefail

IFACE=awg0
SRC=/config/${IFACE}.conf
DST=/etc/amnezia/amneziawg/${IFACE}.conf

if [ ! -f "$SRC" ]; then
  echo "[vpn] ERROR: $SRC not found — mount your AmneziaWG client config there." >&2
  exit 1
fi

mkdir -p /etc/amnezia/amneziawg
install -m 600 "$SRC" "$DST"

down() {
  echo "[vpn] shutting down $IFACE"
  awg-quick down "$IFACE" 2>/dev/null || true
  ip link del "$IFACE" 2>/dev/null || true
  exit 0
}
trap down INT TERM

# Clear any leftover state from a previous run of this same container.
awg-quick down "$IFACE" 2>/dev/null || true
ip link del "$IFACE" 2>/dev/null || true

echo "[vpn] starting $IFACE"
awg-quick up "$IFACE"
awg show "$IFACE"

# Wait for the first handshake so the container only reports ready once traffic
# can actually flow.
for i in $(seq 1 30); do
  hs=$(awg show "$IFACE" latest-handshakes 2>/dev/null | awk '{print $2}')
  if [ -n "${hs:-}" ] && [ "$hs" -gt 0 ]; then
    echo "[vpn] handshake OK"
    break
  fi
  sleep 2
done

# Hold the namespace open; react to signals promptly.
while true; do
  sleep 30 &
  wait $!
done
