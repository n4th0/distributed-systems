# Arquitectura y decisiones de diseño

## 1. Canales de comunicación

| Enlace | Tecnología | Por qué |
|---|---|---|
| Monitor ⇄ CENTRAL | **Sockets** STX/ETX/LRC, sesión persistente | Autenticación y estado; la caída del socket = estación DESCONECTADA |
| Engine ⇄ Monitor | **Sockets** STX/ETX/LRC, sesión persistente | Comprobación de salud cada segundo (hardware/software de la WS) |
| FO, Engine, CENTRAL | **Kafka** (streaming de eventos) | Desacople, asincronía, escalado: N operarios y N estaciones sin tocar CENTRAL |
| CENTRAL ⇄ BD | **SQLite** (`/data/central.db`) | Obligatorio; estaciones, operarios, riegos y auditoría |

## 2. Protocolo de sockets

Trama `<STX><DATA><ETX><LRC>`; `LRC = XOR(bytes de DATA)`; handshake `<ENQ>/<ACK>`; cada trama se confirma con `<ACK>`/`<NACK>`
(hasta 3 reintentos ante LRC erróneo); cierre con `<EOT>`. `DATA = OPERACIÓN#campo1#campo2…`. Implementado en `wm_common/protocol.py`.

**Monitor → CENTRAL**

| Petición | Respuesta | Significado |
|---|---|---|
| `REG#ws_id#ubicación#secreto` | `REG_OK#token` · `REG_KO#BAD_CREDENTIALS / ALREADY_CONNECTED / UNKNOWN_WS / BAD_FORMAT` | Alta (1.ª vez) o autenticación. El secreto se guarda con PBKDF2+sal. El `token` identifica la sesión. |
| `HB#ws_id#token` | `HB_OK` · `KO#REAUTH` | Latido cada 2 s. Sin latidos 10 s ⇒ DESCONECTADA. |
| `LEAK#ws_id#token` | `OK` | Avería (fuga) detectada. |
| `FIX#ws_id#token` | `OK` | Avería resuelta. |

**Engine ⇄ Monitor** (el Engine conecta; luego pregunta el Monitor)

| Petición | Respuesta |
|---|---|
| Engine→Monitor `HELLO#WM_WS_E` | `HELLO_OK#ws_id#token` (identidad validada por CENTRAL) · `HELLO_KO#NOT_REGISTERED` |
| Monitor→Engine `HEALTH#ws_id#token` (cada 1 s) | `OK` · `KO` (o silencio ⇒ avería) |

## 3. Topics de Kafka (mensajes JSON con `ts`)

| Topic | Productor → Consumidor | Clave | Mensajes |
|---|---|---|---|
| `wm.fo.requests` | FO → CENTRAL | operator_id | `{req_id, operator_id, ws_id, duration_s}` |
| `wm.fo.events` | CENTRAL → FO | operator_id | `INFO`, `ACCEPTED`, `DENIED`, `TICK`, `ALERT`, `SUMMARY` (cada FO filtra por su operator_id) |
| `wm.ws.commands` | CENTRAL → Engine | ws_id | `START{req_id,operator_id,duration_s}`, `STOP{reason}`, `BLOCK`, `UNBLOCK` |
| `wm.ws.telemetry` | Engine → CENTRAL | ws_id | `STATUS`(2 s), `TICK`(1 s regando), `STARTED`, `START_DENIED`, `ENDED`, `START_REQ`(riego manual); todos con `token` |
| `wm.ws.status` | CENTRAL → todos | ws_id | `{state, location, flow_lpm, volume_l, operator_id}` (los FO ven todas las WS) |

## 4. Estados de una WS (derivados, sin carreras)

```
no conectada            → DESCONECTADA  (gris)
conectada + fuga        → FUGA          (rojo)
conectada + bloqueada   → FUERA DE SERVICIO (naranja)
conectada + regando     → REGANDO       (verde parpadeante: caudal, volumen, operario)
resto                   → DISPONIBLE    (verde)
```

El estado se calcula a partir de banderas (`connected`, `leak`, `blocked`, `active`), por lo que fuga/bloqueo/desconexión
siempre prevalecen y no hay secuencias de eventos que dejen un estado incoherente. `blocked` se persiste en BD.

## 5. Secuencia de un riego

1. FO publica `wm.fo.requests`. CENTRAL → `INFO` (recibida, operario validado, comprobando disponibilidad).
2. CENTRAL valida: operario activo · WS existe · estado DISPONIBLE · sin reserva previa · Engine vivo. Si no ⇒ `DENIED` con el motivo.
3. CENTRAL reserva la WS (evita doble activación) y publica `START`. Timeout de 8 s si el Engine no confirma.
4. Engine abre la válvula, responde `STARTED`; CENTRAL ⇒ `ACCEPTED` y estado REGANDO.
5. Cada segundo `TICK` (caudal, volumen) → panel de CENTRAL y FO del operario.
6. Fin: tiempo límite (`TIMEOUT`) o `e` en el Engine (`MANUAL`) → `ENDED` → CENTRAL guarda en BD y envía `SUMMARY`.
7. Fuga durante el riego: el Engine cierra al instante (`LEAK`) y el Monitor avisa por socket; CENTRAL envía `STOP`
   y, si el Engine no contesta en 4 s, cierra el riego por su cuenta. Bloqueo, desconexión o pérdida del Monitor se tratan igual.

## 6. Resiliencia

* **Reinicio de CENTRAL**: lee las WS de SQLite y las muestra DESCONECTADAS; los Monitores reconectan solos y se re-autentican.
  Un riego en curso se re-adopta al llegar su telemetría. Consumidor Kafka con *group id* fijo.
* **Anti-replay**: todos los mensajes llevan `ts`; se ignoran los más viejos de 60 s (`WM_MSG_TTL`) y se deduplican los `req_id`.
* **Seguridad de operación**: el Engine cierra la válvula si pierde a su Monitor; CENTRAL reconcilia el bloqueo
  (re-envía `BLOCK/UNBLOCK` si el estado reportado por el Engine difiere del de la BD).
* **Concurrencia**: un `RLock` protege el estado de CENTRAL; un hilo por conexión de socket; un hilo consumidor Kafka; temporizadores para timeouts.

## 7. Seguridad

* Autenticación de WS: secreto por estación (hash PBKDF2-SHA256 con sal, comparación en tiempo constante) + token de sesión aleatorio.
  La telemetría Kafka sin token válido se rechaza y se registra.
* Operarios: solo los dados de alta en la tabla `operators` pueden solicitar riegos.
* Panel web protegido con HTTP Basic (`PANEL_USER` / `PANEL_PASSWORD`) sobre HTTPS de Railway.
* **Límites conocidos (mejoras posibles):** Kafka y los sockets viajan en claro por el TCP Proxy (se podría añadir TLS/SASL);
  el alta automática de WS (`WM_ALLOW_SELF_REGISTRATION=true`) se puede desactivar para entornos reales.
