# VPN (WireGuard) для tg_bot

Контейнер `vpn` — это [linuxserver/wireguard](https://docs.linuxserver.io/images/docker-wireguard/)
в режиме клиента. Контейнер `tg_bot` подключён к нему через
`network_mode: "service:vpn"`, то есть использует его сетевой стек целиком:
весь исходящий трафик бота (в первую очередь — обращения к Telegram API)
идёт через WireGuard-туннель. Обращения к `mongodb` и `functions_api` при
этом продолжают идти напрямую по docker-сети (для неё уже есть более
специфичный маршрут, чем маршрут по умолчанию через `wg0`), так что VPN не
замедляет внутренние запросы бота.

Другие сервисы (`getting_schedule`, `functions_api`, `calendar_service` и
т.д.) через VPN не ходят — согласно задаче, тоннель нужен только боту.

## Настройка

1. Получите клиентский конфиг WireGuard у вашего VPN-провайдера или со
   своего сервера (`wg0.conf`), например:

   ```ini
   [Interface]
   PrivateKey = <ваш приватный ключ>
   Address = 10.0.0.2/32
   DNS = 1.1.1.1

   [Peer]
   PublicKey = <публичный ключ сервера>
   Endpoint = vpn.example.com:51820
   AllowedIPs = 0.0.0.0/0, ::/0
   PersistentKeepalive = 25
   ```

   `AllowedIPs = 0.0.0.0/0, ::/0` обязателен — именно он заставляет весь
   трафик контейнера идти через туннель.

2. Положите файл как `vpn/config/wg0.conf` (каталог уже создан и добавлен в
   `.gitignore`, реальный конфиг с ключами в репозиторий не попадёт).

3. Убедитесь, что на хосте загружен модуль ядра `wireguard`
   (`modprobe wireguard`) и доступен `/dev/net/tun`.

4. Запустите `docker-compose up -d vpn tg_bot` (или весь стек).

Если `vpn/config/wg0.conf` отсутствует, `linuxserver/wireguard` попытается
сам сгенерировать серверный конфиг — это не то, что нужно tg_bot, поэтому
шаг 1-2 обязателен.
