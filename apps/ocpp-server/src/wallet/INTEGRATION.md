# Wallet Integration Contract (CitrineOS v2 wallet fork)

This branch adds a **wallet decision plane** to CitrineOS: five AMQP flows that let an
external wallet/billing app authorize charges, react to charger state, and push remote
commands — without modifying any upstream CitrineOS file. This document is the contract
an integrator implements. The source of truth is the code in this folder
(`WalletRpcClient.ts`, `gates.ts`, `consumers.ts`); this file restates it for humans.

Transport: RabbitMQ (same broker CitrineOS uses). All bodies are JSON, `contentType:
application/json`, non-persistent. The CSMS side is enabled per-flow with env flags:

| Flow | Direction | Queue (default) | Enable flag |
|---|---|---|---|
| 1. Authorize | CSMS → wallet (RPC) | `citrineos.rabbitmq.auth` | `RABBITMQ_AUTH=true` |
| 2. Preparing | CSMS → wallet (RPC) | `citrineos.rabbitmq.preparing` | `RABBITMQ_PREPARING=true` |
| 3. SuspendedEV | CSMS → wallet (RPC) | `citrineos.rabbitmq.suspended` | `RABBITMQ_SUSPENDED=true` |
| 4. RemoteStart | wallet → CSMS (cmd) | `citrineos.rabbitmq.remotestart` | `RABBITMQ_REMOTESTART=true` |
| 5. RemoteStop | wallet → CSMS (cmd) | `citrineos.rabbitmq.remotestop` | `RABBITMQ_REMOTESTOP=true` |

Queue names are overridable via `RABBITMQ_AUTH_QUEUE`, `RABBITMQ_PREPARING_QUEUE`,
`RABBITMQ_SUSPENDED_QUEUE`, `RABBITMQ_REMOTESTART_QUEUE`, `RABBITMQ_REMOTESTOP_QUEUE`.

## RPC mechanics (flows 1–3, CSMS is the caller)

The CSMS publishes a request to the named durable queue with:
- `replyTo` = a server-named **exclusive** queue (consume it as-is, `noAck`)
- `correlationId` = uuid — echo it **verbatim** on the reply
- timeout: `RABBITMQ_TIMEOUT_MS` (default 10000 ms). Reply after the timeout is ignored.

Your wallet app consumes the request queue, decides, and publishes the reply to
`replyTo` with the same `correlationId`. One reply per request.

### Flow 1 — Authorize (every idToken presented at a charger)
Request:
```json
{ "tenantId": 1, "stationId": "STATION01", "idToken": "AABBCCDDEEFF", "idTokenType": "MacAddress" }
```
`idTokenType` is `MacAddress` for MAC-shaped tokens (autocharge), else `ISO14443`,
or whatever the charger declared. Reply:
```json
{ "status": "Accepted", "cacheExpiryDateTime": null }
```
- `status` is an OCPP `AuthorizationStatusEnum` value. On **OCPP 1.6 stations** only
  `Accepted | Blocked | Expired | Invalid | ConcurrentTx` survive the wire — anything
  richer is clamped to `Invalid` by the CSMS, so prefer these five.
- `cacheExpiryDateTime` (ISO string) is optional; when present it bounds how long the
  CSMS may reuse this decision as a fallback cache. **The wallet is consulted on every
  fresh Authorize** — the cache is only used when the wallet is unreachable.
- Timeout / no reply ⇒ the CSMS **fails closed** (`Invalid`) unless it holds a cached
  decision for the same (token, station, evse).

### Flow 2 — Preparing (a car plugged in; should a pre-authorized session start?)
Fires when a connector goes `Preparing` (1.6) / `Occupied` (2.x) and **no transaction
is active on that connector**. Request:
```json
{ "tenantId": 1, "stationId": "STATION01", "evseId": 1, "connectorId": 1 }
```
Reply — either reject, or accept with the token to start with:
```json
{ "status": "Accepted", "idTag": "W1234567890", "idTokenType": "Central" }
{ "status": "Rejected" }
```
On `Accepted` the CSMS upserts the token as authorized and dispatches
RemoteStartTransaction (1.6) / RequestStartTransaction (2.x) on that connector.
Expect **repeat asks** for the same plug event (dedupe window ≈ RPC timeout + 30 s);
reply `Rejected` when you have nothing pending — it is cheap and idempotent.

### Flow 3 — SuspendedEV (car stopped drawing power; keep or stop the session?)
Fires once per transaction (Redis-deduped), after a debounce
(`RABBITMQ_SUSPENDED_MIN_DURATION_MS`, default 30000 ms) and a DB re-check that the
transaction is still active and still suspended. Request:
```json
{ "tenantId": 1, "stationId": "STATION01", "evseId": 1, "transactionId": "42", "connectorId": 1 }
```
Reply:
```json
{ "action": "Stop" }     // CSMS dispatches RemoteStop for that transaction
{ "action": "Continue" } // leave it be
```

## Command queues (flows 4–5, the wallet is the caller)

Publish to the durable queue. If you set `replyTo` (+ `correlationId`), the CSMS sends
back the OCPP dispatch confirmation (`IMessageConfirmation`, or
`{"success": false, "payload": "<error>"}`); without `replyTo` it is fire-and-forget.
Messages are **always acked** — failures come back on `replyTo`, nothing is requeued.

### Flow 4 — RemoteStart
```json
{ "stationId": "STATION01", "idTag": "W1234567890", "evseId": 1 }
```
- `connectorId` is accepted as an alias of `evseId`. Optional `chargingProfile` is
  passed through. Advanced: send `request` to supply the raw OCPP payload verbatim.
- The CSMS pre-authorizes `idTag` (upserts an Accepted authorization) before
  dispatching, so the charger's follow-up Authorize/StartTransaction succeeds.

### Flow 5 — RemoteStop
```json
{ "stationId": "STATION01", "transactionId": "42" }
```
Idempotent by design: unknown tx ⇒ `{"success": false, "payload": "Unknown transaction: …"}`;
already stopped ⇒ `{"success": true, "payload": {"alreadyStopped": true, "endedAt": …, "stoppedReason": …}}`
with **no** OCPP dispatch.

## Reading live/billing state (Hasura)

CitrineOS ships Hasura over its Postgres. Useful reads for a wallet:
- `Transactions`: `ocppConnectionName` (station id on the wire), `transactionId`,
  `isActive`, `chargingState`, `totalKwh` (**session** energy, already
  register−meterStart, in kWh), `meterStart`.
- `MeterValues`: raw sampled values — note `Energy.Active.Import.Register` is the
  charger's **lifetime odometer**, never bill it directly; bill `totalKwh`.
- `StatusNotifications`: connector status stream. Order by **`id: desc`** (NOT
  `timestamp` — it is NULL on every row in beta4) and `distinct_on` per connector.

## Gotchas that will bite you (all field-verified)

1. **OCPP transaction ids repeat across stations** (v2 unique is
   `(stationId, transactionId)`, and 1.6 chargers hand out small integers). Never key
   anything on `transactionId` alone — always `(stationId, transactionId)`.
2. **2.0.1 idToken format**: ISO14443 must be 8/14 hex chars; arbitrary strings are
   rejected by the CSMS's payload validation before your wallet is ever consulted.
3. **Authorization link timing**: the transaction row's link to its authorization (and
   therefore the idToken) can land a beat *after* the transaction start event. If you
   correlate sessions by token, treat the token as reliable *late* (first meter tick
   onward), not at start.
4. `cacheExpiryDateTime` on a *stored* authorization that has expired causes the CSMS
   to reject a **new** transaction before consulting authorizers — leave it null in
   replies unless you intend exactly that.
5. Repeat Preparing asks and duplicate SuspendedEV events are normal under
   at-least-once delivery — make every reply idempotent.

## Minimal consumer skeleton (Node, amqplib)

```js
const amqp = require('amqplib');
const conn = await amqp.connect(process.env.AMQP_URL);
const ch = await conn.createChannel();
for (const q of ['citrineos.rabbitmq.auth', 'citrineos.rabbitmq.preparing', 'citrineos.rabbitmq.suspended']) {
  await ch.assertQueue(q, { durable: true });
}
ch.consume('citrineos.rabbitmq.auth', async (msg) => {
  const req = JSON.parse(msg.content.toString());
  const decision = await decide(req);            // your wallet logic
  ch.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(decision)), {
    correlationId: msg.properties.correlationId, contentType: 'application/json',
  });
  ch.ack(msg);
});
```

## Running this CSMS

`deploy/k8s/citrineos/` in this repo is a complete k8s manifest set (run-once migrate
Job included). The modules deployment starts the wallet entrypoint with
`command: node dist/wallet/main.js` and the flow flags above. Compose users: build
`apps/ocpp-server/deploy.Dockerfile` and set the same env on the `citrine` service.
