# VPN (AmneziaWG) для tg_bot

Контейнер `vpn` собирается из `vpn/Dockerfile` (база —
[`amneziavpn/amneziawg-go`](https://github.com/amnezia-vpn/amneziawg-go),
userspace-реализация, **модуль ядра не требуется**). Контейнер `tg_bot`
подключён к нему через `network_mode: "service:vpn"` и использует его сетевой
стек целиком: весь исходящий трафик бота (в первую очередь — обращения к
Telegram API) идёт через обфусцированный AmneziaWG-туннель. Обращения к
`mongodb` и `functions_api` продолжают идти напрямую по docker-сети (для неё
есть более специфичный маршрут, чем маршрут по умолчанию через `awg0`).

Другие сервисы (`getting_schedule`, `functions_api`, `calendar_service`)
через VPN не ходят.

## Настройка

1. Получите клиентский конфиг AmneziaWG (`awg0.conf`) — с параметрами
   `Jc/Jmin/Jmax`, `S1..S4`, `H1..H4`, при необходимости `I1..I5`.

2. Положите его как `vpn/config/awg0.conf` (каталог в `.gitignore`, реальный
   конфиг с ключами в репозиторий не попадает).

3. `AllowedIPs` должен покрывать весь трафик: `0.0.0.0/0`. IPv6 (`::/0`) в
   контейнере отключён — kill-switch по ip6tables там неприменим; для бота
   достаточно IPv4.

4. На хосте нужен `/dev/net/tun` (обычно есть по умолчанию). Модуль ядра
   `wireguard`/`amneziawg` не требуется — используется userspace
   `amneziawg-go`.

5. `docker compose up -d --build vpn tg_bot` (или весь стек).

Проверка: `docker compose exec vpn awg show` — должен быть свежий
`latest handshake`; `docker compose exec vpn wget -qO- https://ifconfig.me/ip`
покажет IP выходного узла AmneziaWG, а не сервера.
