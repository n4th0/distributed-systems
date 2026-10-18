# WaterManagement — SD 26/27

Sistema distribuido de gestión centralizada del riego de una ciudad:
**CENTRAL** (Railway) + **Kafka** (Railway) + **SQLite**, y en el laboratorio las **Watering Stations**
(Monitor + Engine) y los **Field Operators**. Todo en Python 3.11 y desplegado con Docker.

```
                       ┌───────────────────── Railway ─────────────────────┐
  PC LAB 2             │  WM_Central ──(SQLite /data)    Kafka (KRaft)      │            PC LAB 1
  ┌───────────────┐    │   ▲ sockets :9000 (TCP proxy)    ▲  :9094 (proxy)  │    ┌────────────────┐
  │ WM_WS_M ──────┼────┼───┘  STX/ETX/LRC                 │  :9092 interno   │    │ WM_FO (1)      │
  │  ▲ sockets    │    │   Panel web :8080 (HTTPS)        │                  │    │ WM_FO (2)      │
  │ WM_WS_E ──────┼────┼──────── Kafka (eventos) ─────────┘◄─────────────────┼────┤ (Kafka)        │
  └───────────────┘    └────────────────────────────────────────────────────┘    └────────────────┘
```

| Módulo         | Fichero                 | Comunicación                                                                |
| -------------- | ----------------------- | --------------------------------------------------------------------------- |
| CENTRAL        | `central/WM_Central.py` | Sockets con los Monitores · Kafka con Engines y FO · SQLite · panel web     |
| Monitor        | `ws/WM_WS_M.py`         | Sockets con CENTRAL (autenticación, latido, avería) y con su Engine (salud) |
| Engine         | `ws/WM_WS_E.py`         | Sockets con su Monitor · Kafka con CENTRAL (órdenes y telemetría)           |
| Field Operator | `fo/WM_FO.py`           | Kafka (peticiones, progreso, resumen, estado de todas las WS)               |

Detalle de protocolos, topics, estados y decisiones de diseño: [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md).

## Estructura del repositorio

```
central/   WM_Central.py, central_db.py, static/index.html (panel), Dockerfile, requirements.txt
ws/        WM_WS_M.py, WM_WS_E.py, Dockerfile.monitor, Dockerfile.engine
fo/        WM_FO.py, Dockerfile, services/*.csv (ficheros de servicios de ejemplo)
kafka/     Dockerfile + entrypoint.sh (Kafka KRaft preparado para Railway)
wm_common/ protocol.py (STX/ETX/LRC), bus.py (Kafka), topics.py, util.py
tests/     test_system.py (protocolo + sistema completo con bus en memoria)
docker-compose.local.yml     todo el sistema en tu PC (prueba previa)
docker-compose.lab-ws.yml    PC LAB 2: WS 1 y 2
docker-compose.lab-fo.yml    PC LAB 1: FO 1 y 2
docs/      ARQUITECTURA.md, PROMPTS.md
```

---

## 1. Prueba local (antes de desplegar)

Requisitos: Docker Desktop (o Docker Engine + Compose v2).

```bash
docker compose -f docker-compose.local.yml up --build
# Panel: http://localhost:8080   usuario: admin   contraseña: admin
```

En otra terminal:

```bash
docker attach wm-engine-1                 # menú del Engine (salir sin pararlo: Ctrl+P, Ctrl+Q)
docker compose -f docker-compose.local.yml run --rm fo1     # operario FO-01 (menú interactivo)
# o con fichero de servicios y sin menú:
docker compose -f docker-compose.local.yml run --rm fo1 python WM_FO.py --file services/fo1.csv --no-menu
```

Tests automáticos (no necesitan Docker ni Kafka):

```bash
pip install flask waitress
python -m unittest discover -s tests -v
```

---

## 2. Despliegue en la nube (Railway) — CENTRAL + BD + KAFKA

### 2.0 Repositorio

1. Crea un repositorio en GitHub y sube **todo este proyecto** (`git init && git add . && git commit -m "WaterManagement" && git push`).
2. _Settings → Collaborators_: da acceso a tu profesor de prácticas.
3. Sube también `docs/PROMPTS.md` completado con los prompts que hayas usado con IA (lo exige el enunciado).

### 2.1 Proyecto y servicio **kafka**

1. [railway.com](https://railway.com) → **New Project → Empty Project**.
2. **+ New → GitHub Repo →** tu repositorio. Renombra el servicio a **`kafka`** (importante: el nombre forma el dominio privado `kafka.railway.internal`).
3. _Variables_:
   | Variable                  | Valor                                  |
   | ------------------------- | -------------------------------------- |
   | `RAILWAY_DOCKERFILE_PATH` | `kafka/Dockerfile`                     |
   | `RAILWAY_RUN_UID`         | `0` _(permite escribir en el volumen)_ |
4. _Settings → Volumes (o clic derecho en el servicio → Attach volume)_: monta un volumen en **`/var/lib/kafka/data`**.
5. _Settings → Networking → TCP Proxy → Add TCP Proxy_, **puerto de aplicación `9094`**. Railway te da `algo.proxy.rlwy.net:NNNNN` → **ese es `KAFKA_PUBLIC_ADDR`** (apúntalo).
6. Pulsa **Deploy / Redeploy** (el entrypoint toma `RAILWAY_TCP_PROXY_DOMAIN/PORT` para anunciar la dirección pública, por eso hay que redesplegar _después_ de crear el proxy).
7. En _Deploy Logs_ debe verse `Kafka advertised listeners: INTERNAL://kafka.railway.internal:9092,EXTERNAL://algo.proxy.rlwy.net:NNNNN` y, al final, `Kafka Server started`.

> Si las variables `RAILWAY_TCP_PROXY_*` no aparecen en el contenedor, define a mano `KAFKA_EXTERNAL_HOST` y `KAFKA_EXTERNAL_PORT` con el dominio y puerto del proxy.

### 2.2 Servicio **central**

1. **+ New → GitHub Repo →** el mismo repositorio. Renómbralo a **`central`**.
2. _Variables_:
   | Variable                  | Valor                                                                        |
   | ------------------------- | ---------------------------------------------------------------------------- |
   | `RAILWAY_DOCKERFILE_PATH` | `central/Dockerfile`                                                         |
   | `KAFKA_BROKER`            | `kafka.railway.internal:9092`                                                |
   | `PORT`                    | `8080`                                                                       |
   | `WM_SOCKET_PORT`          | `9000`                                                                       |
   | `PANEL_USER`              | `admin`                                                                      |
   | `PANEL_PASSWORD`          | _(elige una contraseña)_                                                     |
   | `WM_OPERATORS`            | `FO-01,FO-02,FO-03,FO-04` _(operarios dados de alta; añade los que quieras)_ |
3. _Volumes_: volumen en **`/data`** (aquí vive `central.db`, la base SQLite; así sobrevive a reinicios y redeploys).
4. _Networking_:
   - **Generate Domain** → puerto **8080** → URL HTTPS del panel.
   - **TCP Proxy** → puerto de aplicación **`9000`** → `otro.proxy.rlwy.net:MMMMM` → **ese es `CENTRAL_SOCKET_ADDR`**.
5. _Settings → Healthcheck Path_: `/healthz` (opcional pero recomendable).
6. Deploy. En los logs: `Servidor de sockets escuchando en el puerto 9000`, `Conectado al broker kafka.railway.internal:9092`, `Panel web en el puerto 8080`.

CI/CD resultante: **VS Code → GitHub → Railway** (cada `git push` a `main` redespliega `kafka` y `central`).

---

## 3. Despliegue en el laboratorio (solo FO y WS)

<!-- En **ambos PC**: Docker + Git instalados. -->
<!---->
<!-- ```bash -->
<!-- git clone https://github.com/<usuario>/<repo>.git && cd <repo> -->
<!-- cp .env.example .env     # edítalo con KAFKA_PUBLIC_ADDR y CENTRAL_SOCKET_ADDR de Railway -->
<!-- ``` -->
<!---->
<!-- **PC LAB 2 — estaciones (2 WS, cada una con Monitor + Engine):** -->
<!---->
<!-- ```bash -->
<!-- docker compose -f docker-compose.lab-ws.yml --env-file .env up --build -d -->
<!-- docker attach wm-engine-1        # menú del Engine 1 (Ctrl+P, Ctrl+Q para soltarlo) -->
<!-- docker attach wm-engine-2 -->
<!-- ``` -->

# Construir una vez
docker compose build

# Monitors (en segundo plano)
docker compose run -d --rm --name wm-monitor-1 -e WM_WS_ID=WS-01 -e WM_WS_LOCATION="River Park" -e WM_WS_SECRET=s3cret-1 monitor
docker compose run -d --rm --name wm-monitor-2 -e WM_WS_ID=WS-02 -e WM_WS_LOCATION="Central Park" -e WM_WS_SECRET=s3cret-2 monitor
docker compose run -d --rm --name wm-monitor-3 -e WM_WS_ID=WS-03 -e WM_WS_LOCATION="Lake" -e WM_WS_SECRET=s3cret-3 monitor

# Engines (interactivos, uno por terminal), conectados al monitor que quieras
docker compose run --rm --name wm-engine-1 -e WM_MONITOR=wm-monitor-1:7001 engine
docker compose run --rm --name wm-engine-2 -e WM_MONITOR=wm-monitor-3:7001 engine   # otro engine al monitor 3
docker compose run --rm --name wm-engine-3 -e WM_MONITOR=wm-monitor-1:7001 engine   # dos engines al mismo monitor

# Parar un monitor
docker stop wm-monitor-2




**PC LAB 1 — operarios (2 FO):** abre dos terminales:

```bash
docker compose -f docker-compose.lab-fo.yml --env-file .env build
docker compose -f docker-compose.lab-fo.yml --env-file .env run --rm fo1     # FO-01 (menú)
docker compose -f docker-compose.lab-fo.yml --env-file .env run --rm fo2     # FO-02 (menú)
# modo fichero (automático, 4 s entre servicios):
docker compose -f docker-compose.lab-fo.yml --env-file .env run --rm fo1 python WM_FO.py --file services/fo1.csv --no-menu
```

Para añadir más estaciones u operarios: duplica un bloque `monitorN/engineN` cambiando `WM_WS_ID`, ubicación, secreto y
puerto interno (cada pareja usa su propia red de contenedores por nombre, no hace falta cambiar el 7001), o añade ids a `WM_OPERATORS`.





















---

## 4. Guion de demostración (checklist de la corrección)

1. **Arranque/reinicio de CENTRAL**: reinicia el servicio en Railway → el panel muestra las WS de la BD en **gris/Desconectada** hasta que sus Monitores reconectan solos (reintento cada 3 s).
2. **Alta y autenticación**: arranca `monitor1`/`engine1` → en el panel aparece `WS-01` verde. Un Monitor con secreto incorrecto es rechazado (log `AUTH`).
3. **Riego por operario**: en `fo1` → _Solicitar riego_ `WS-01`, 20 s → se ven los pasos en FO **y** en el panel (_Mensajes de la aplicación_): recibida → operario validado → comprobando → autorizado; fila **verde parpadeante** con caudal, volumen y operario cada segundo; resumen final al terminar.
4. **Denegaciones**: estación inexistente, estación ocupada (segundo operario a la vez), Fuera de servicio, Fuga, Desconectada, operario no registrado.
5. **Riego manual en la estación**: en el Engine, `s 15`; `e` para finalizar antes de tiempo.
6. **Fuga**: durante un riego, en el Engine pulsa `k` → el riego se corta al instante, el Monitor avisa a CENTRAL, fila **roja**; operario notificado. `k` otra vez → vuelve a **verde**.
7. **Órdenes de CENTRAL** (botones del panel, a una WS o a _Todas_): _Iniciar riego_, _Bloquear WS_ (naranja “Fuera de Servicio”; corta el riego en curso) y _Activar WS_.
8. **Desconexión**: para el contenedor `monitor1` → `WS-01` pasa a **gris**; un riego en curso se cierra y se informa al operario.
9. **Modo fichero**: `--file services/fo1.csv` con 2 FO a la vez (asincronía, sin turnos).

## 5. Variables de entorno (resumen)

| Módulo  | Variables                                                                                                                                                                                  |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| central | `KAFKA_BROKER`, `WM_SOCKET_PORT`, `PORT`, `WM_DB_PATH`, `PANEL_USER`, `PANEL_PASSWORD`, `WM_OPERATORS`, `WM_HB_TIMEOUT`, `WM_DEFAULT_DURATION`, `WM_ALLOW_SELF_REGISTRATION`, `WM_MSG_TTL` |
| WM_WS_M | `WM_CENTRAL`, `WM_ENGINE_PORT`, `WM_WS_ID`, `WM_WS_LOCATION`, `WM_WS_SECRET`                                                                                                               |
| WM_WS_E | `KAFKA_BROKER`, `WM_MONITOR`, `WM_BASE_FLOW`, `WM_DEFAULT_DURATION`                                                                                                                        |
| WM_FO   | `KAFKA_BROKER`, `WM_OPERATOR_ID`, `WM_SERVICES_FILE`, `WM_NO_MENU`, `WM_DEFAULT_DURATION`                                                                                                  |

Todos aceptan también argumentos por línea de comandos (`--help`), como pide el enunciado.

## 6. Problemas habituales

| Síntoma                                                    | Causa / solución                                                                                                                     |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| FO/Engine: `Esperando al broker ...`                       | `KAFKA_PUBLIC_ADDR` mal copiado, o Kafka no se redesplegó tras crear el TCP Proxy. Comprueba el log `advertised listeners` de Kafka. |
| Kafka no arranca / `Permission denied` en el volumen       | Falta `RAILWAY_RUN_UID=0` en el servicio kafka.                                                                                      |
| Central: `Esperando al broker kafka.railway.internal:9092` | El servicio de Kafka no se llama exactamente `kafka`, o aún está arrancando (Central reintenta solo).                                |
| Monitor: `Sin enlace con CENTRAL`                          | `CENTRAL_SOCKET_ADDR` debe ser el **TCP Proxy del puerto 9000** (no la URL HTTPS del panel).                                         |
| `REG_KO#BAD_CREDENTIALS`                                   | La WS ya existe con otro secreto. Usa el original o borra `/data/central.db` (volumen) para empezar de cero.                         |
| `REG_KO#ALREADY_CONNECTED`                                 | Hay otro Monitor con ese ID conectado; para el anterior (o espera 10 s a su timeout).                                                |
| No puedo escribir en el Engine                             | Lanza con `tty: true / stdin_open: true` (compose ya lo hace) y usa `docker attach`.                                                 |
| El riego se deniega con “El Engine no responde”            | El Engine no está enviando STATUS: revisa que esté conectado a su Monitor y a Kafka.                                                 |

## 7. Memoria a entregar (UACloud) — qué incluir

Portada (nombres, apellidos, DNI, curso, título) · componentes y descripción de cada uno + código fuente ·
guía de despliegue paso a paso (esta misma, secciones 2 y 3) · capturas de pantalla (panel con los 5 estados, FO con
el proceso, Engine, logs de Railway) · código fuente y compilados · prompts de IA en el repo.
**Fecha de entrega: semana del 26 de octubre de 2026.**
