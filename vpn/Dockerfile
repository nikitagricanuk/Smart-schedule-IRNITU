# AmneziaWG client sidecar (userspace amneziawg-go — no kernel module needed).
# tg_bot shares this container's network namespace, so all of its outbound
# traffic (Telegram API in particular) is obfuscated through the AmneziaWG
# tunnel described in vpn/config/awg0.conf.
FROM amneziavpn/amneziawg-go:latest

RUN set -eux; \
    apk add --no-cache bash iproute2 iptables; \
    # A `DNS =` line in the config makes awg-quick call `resolvconf`. We must
    # NOT let it rewrite /etc/resolv.conf: tg_bot shares this container's netns
    # and needs Docker's embedded resolver (127.0.0.11) to reach `mongodb` /
    # `functions_api` by name. Ship a no-op shim; the tunnel's default route
    # already carries the bot's traffic, VPN-side DNS is not needed.
    printf '%s\n' '#!/bin/bash' 'exit 0' > /usr/local/bin/resolvconf; \
    chmod +x /usr/local/bin/resolvconf; \
    # src_valid_mark is set for us via the container's `sysctls:` — awg-quick's
    # own write to it fails on the read-only /proc/sys and aborts bring-up.
    sed -i 's/.*net\.ipv4\.conf\.all\.src_valid_mark.*/\t\ttrue/' "$(command -v awg-quick)"

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=4 \
  CMD bash -c 'hs=$(awg show awg0 latest-handshakes 2>/dev/null | awk "{print \$2}"); [ -n "$hs" ] && [ "$hs" -gt 0 ] && [ $(( $(date +%s) - hs )) -lt 200 ]'

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
