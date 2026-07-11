# IPTV Balance

Автоподбор лучшего сервера и плейлист для IPTV-провайдера **new.tv.team**.
Сервис измеряет скорость всех серверов `*.hls.gd` (браузерный speedtest,
повторяющий алгоритм кабинета), выбирает лучший, применяет его в личном кабинете
и проксирует плейлист/потоки через себя — плеер всегда играет с актуального
сервера без «заглушек».

Чистая стандартная библиотека Python — без `pip install`.

## Возможности

- **Браузерный speedtest** по алгоритму кабинета new.tv.team:
  `ping` (медиана 10 RTT) + `jitter` + `download` (10с) + `HLS` (5 сегментов) →
  `score` и emoji-рейтинг (👍👍👍/👍👍/👍/❌).
- **Автоприменение** лучшего сервера в кабинете (`groupId`) с прогревом плейлиста
  (warm-apply: переключение происходит только когда новый сервер реально готов,
  без окна «обновите плейлист»).
- **Прокси потоков** `/p/<channel>/<file>` — токены всегда свежие и привязаны к
  текущему серверу кабинета.
- **Дашборд**: выбор плейлиста из кабинета (dropdown), адрес подключения,
  кабинет-логин с капчей, список серверов (карточки/строки), тест, авто-выбор.
- Эндпоинты: `/` (дашборд), `/plst.m3u8` (плейлист для плеера), `/p/...` (прокси),
  `/api/status`, `/api/report`, `/api/cabinet/*`.

## Файлы

- `server.py` — сервис (один файл).
- `servers.json` — список серверов с `groupId` кабинета.
- `config.example.json` — шаблон конфигурации (без секретов).
- `logo.png` — логотип/фавикон.
- `Dockerfile`, `docker-compose.yml`, `.github/workflows/docker.yml` — Docker и CI.

## Запуск через Docker

```bash
docker run -d --name iptv-balance \
  -p 80:80 \
  -v iptv-data:/data \
  --restart unless-stopped \
  alexkuryshko/iptv-balance:latest
```

или через docker-compose:

```bash
docker compose up -d
```

Откройте `http://<IP-сервера>/`, войдите в кабинет new.tv.team (логин/пароль +
капча), выберите плейлист (по умолчанию — TiviMate), укажите адрес подключения —
и вставьте в плеер `http://<адрес>/plst.m3u8`.

Конфиг, cookies и логи хранятся в томе `/data` (`DATA_DIR`), образ не содержит
секретов. Порт/хост можно переопределить переменными `PORT` / `HOST`.

## Запуск без Docker

```bash
python3 server.py
# dashboard:  http://<ip>/
# playlist:   http://<ip>/plst.m3u8
```

Конфиг — `config.json` (создаётся из `config.example.json`). Порт — `listen_port`
или переменная `PORT`.

## Публикация (Docker Hub + GitHub)

1. **GitHub**: запушьте репозиторий.
   ```bash
   git init && git add . && git commit -m "IPTV Balance"
   git remote add origin git@github.com:cybrp/iptv-balance.git
   git push -u origin main
   ```
2. **Docker Hub**: создайте репозиторий `cybrp/iptv-balance` и Access Token
   (Account Settings → Security).
3. **GitHub Secrets**: в репозитории Settings → Secrets → Actions добавьте:
   - `DOCKERHUB_USERNAME` — ваш логин Docker Hub (`cybrp`)
   - `DOCKERHUB_TOKEN` — Access Token из Docker Hub
4. **CI**: push в `main` (или тег `v*`) запустит `.github/workflows/docker.yml`,
   который соберёт мультиарх (`linux/amd64`, `linux/arm64`) образ и опубликует его
   в Docker Hub как `cybrp/iptv-balance:latest` (+ по SHA и тегу).

Ручная публикация без CI:

```bash
docker buildx create --use
docker login
docker buildx build --platform linux/amd64,linux/arm64 \
  -t alexkuryshko/iptv-balance:latest --push .
```

## Эндпоинты

| URL                | Что делает                                                       |
|--------------------|------------------------------------------------------------------|
| `/`                | Дашборд: рейтинг серверов, кабинет, тест, конфиг                 |
| `/plst.m3u8`       | Готовый плейлист (абсолютные ссылки на адрес подключения)        |
| `/p/<ch>/<file>`   | Прокси потока к текущему серверу кабинета (свежие токены)        |
| `/logo.png`        | Логотип/фавикон                                                  |
| `/api/status`      | JSON со всеми замерами и состоянием                              |
| `/api/report`      | POST `{measurements:[...]}` — браузерные замеры (score/ping/...) |
| `/api/cabinet/*`   | Логин/капча/выбор сервера/apply-best/playlists                   |

## Как это работает

1. Кабинет-интеграция: логин с капчей → `groupId` текущего сервера →
   `/v3/playlists/item` отдаёт плейлист со свежими сервер-связанными токенами.
2. Браузерный speedtest (зеркало кабинета) измеряет каждый сервер; лучший по
   `score` (с фильтром качества) применяется в кабинете.
3. Прокси `/p/...` переписывает ссылки плейлиста на себя и динамически резолвит
   `(host, token)` канала из карты, обновляемой из кабинета.
4. Warm-apply после смены сервера: прокси продолжает отдать со старого сервера,
   пока новый не подтвердит реальными сегментами — бесшовное переключение.
