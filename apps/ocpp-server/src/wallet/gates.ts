// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — flows 2 & 3, triggered from the upstream `messages`
// topic exchange (durable; the router publishes every OCPP frame with routing key
// `frame.<direction>.<Action>`), NOT from repository CRUD events. Frames carry
// `ocppConnectionName` + OCPP-numbered connector/evse ids directly, so the trigger
// path needs no DB-FK resolution and cannot be broken by upstream schema refactors.
// We bind ONE shared durable queue (`wallet.gates`) — each frame reaches exactly one
// replica; that is safe because every RPC-firing path re-reads current DB state
// first (active-tx guard in PreparingGate; tx/connector re-read after the
// SuspendedEV debounce), so an arm on replica A survives its cancel landing on
// replica B: the post-debounce re-read sees the resumed/ended state and bails.
//
// ID SEMANTICS: frames carry OCPP wire numbers; DB rows carry PKs/FKs.
//   Connectors.connectorId   = OCPP 1.6 per-station serial (the number on the wire)
//   Connectors.evseId        = FK → Evses.id (DB PK — NOT an OCPP number)
//   Evses.evseTypeId         = OCPP 2.0.1 evse serial
//   Transactions.connectorId = FK → Connectors.id;  Transactions.evseId = FK → Evses.id
// The wallet contract's `evseId` is ChargeMai's evse numbering == the 1.6
// per-station connector serial (1 connector per EVSE fleet-wide), so gates resolve
// FKs down to Connectors.connectorId / Evses.evseTypeId before putting anything on
// the wire. Never send a raw FK.
//
// Wire contract (1.9.1, wallet side unchanged):
//   preparing: req {tenantId, stationId, evseId, connectorId?}
//              rep {status:'Accepted'|'Rejected', idTag, idTokenType}
//   suspended: req {tenantId, stationId, evseId, transactionId, connectorId?}
//              rep {action:'Stop'|'Continue'}

import { sequelize as dalSequelize } from '@citrineos/dal';
const { Connector, Evse, ChargingStation } = dalSequelize;
import { FrameDirection, MESSAGES_EXCHANGE } from '@citrineos/types';
import type { ILogObj, Logger } from 'tslog';
import { AmqpRpc, RPC_TIMEOUT_MS } from './WalletRpcClient.js';
import type { WalletAuthorizationRepository } from './WalletAuthorizationRepository.js';

const PREPARING_QUEUE = process.env.RABBITMQ_PREPARING_QUEUE ?? 'citrineos.rabbitmq.preparing';
const SUSPENDED_QUEUE = process.env.RABBITMQ_SUSPENDED_QUEUE ?? 'citrineos.rabbitmq.suspended';
const SUSPENDED_MIN_DURATION_MS = Number(process.env.RABBITMQ_SUSPENDED_MIN_DURATION_MS ?? 30000);
const CACHE_NS = 'walletGates';

export interface GateDeps {
  channelManager: any;
  logger: Logger<ILogObj>;
  cache: any;
  locationRepository: any;
  transactionEventRepository: any;
  authorizationRepository: WalletAuthorizationRepository;
  ocppSender: any;
}

// Frame event as published on the `messages` exchange (subset we consume).
export interface GateFrameEvent {
  tenantId: number;
  ocppConnectionName: string;
  protocol: string;
  action?: string;
  parsed: boolean;
  payload?: any;
}

// name → ChargingStations.id, cached per process. Station rows are created once at
// first boot and never re-keyed, so a plain Map is safe (no eviction needed at our
// fleet size; a rename would need a pod restart, same as a config change).
const _stationIds = new Map<string, number>();
async function stationDbIdByName(tenantId: number, name: string): Promise<number | undefined> {
  const key = `${tenantId}:${name}`;
  const hit = _stationIds.get(key);
  if (hit != null) return hit;
  const st = await ChargingStation.findOne({
    where: { tenantId, ocppConnectionName: name },
  }).catch(() => null);
  if (st?.id != null) _stationIds.set(key, st.id);
  return st?.id ?? undefined;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Frames are published on RECEIPT — before the StatusNotification handler commits
// the Connector/Evse rows — and StatusNotification is edge-triggered, so a missed
// FIRST plug on a never-provisioned connector is NOT re-sent. Retry on a backoff
// long enough to cover cold-connector provisioning (row creation) before giving up.
// Only the pre-commit callers (preparing onFrame) need this; a post-debounce caller
// (30s later) passes attempts=1 — if the row is missing by then, retry won't help.
const ROW_RETRY_DELAYS_MS = [400, 800, 1500, 2500];

// Drift detector: connectorRowFromFrame returning null for EVERY frame is the real
// symptom of an upstream payload/schema change (frames still parse, the gates just
// resolve nothing) — the one failure mode always-ack + per-frame warns would hide.
// Count consecutive misses across both gates (one process-wide chokepoint) and
// escalate warn→error; reset on any hit. A single misconfigured charger spamming an
// unknown connector will also trip it — which is itself worth an error.
let _consecutiveRowMisses = 0;
function _noteRowResolution(logger: Logger<ILogObj>, hit: boolean): void {
  if (hit) {
    _consecutiveRowMisses = 0;
    return;
  }
  _consecutiveRowMisses++;
  if (_consecutiveRowMisses === 25) {
    logger.error(
      `gates: ${_consecutiveRowMisses} consecutive frames resolved NO connector row — ` +
        `likely an upstream payload/schema drift or a persistently misconfigured station.`,
    );
  }
}

// Resolve the Connector DB row a frame refers to, via the upstream location
// repository (tenant-aware, resolves ocppConnectionName→stationId itself, and
// includes the Evse). `attempts` bounds the cold-start retry (1 = single-shot).
async function connectorRowFromFrame(
  deps: GateDeps,
  evt: GateFrameEvent,
  attempts = ROW_RETRY_DELAYS_MS.length + 1,
): Promise<any | null> {
  const { tenantId, ocppConnectionName: name } = evt;
  const repo = deps.locationRepository;
  const p = evt.payload ?? {};
  const lookup = async (): Promise<any | null> => {
    if (evt.protocol === 'ocpp1.6') {
      // 1.6: payload.connectorId is the per-station serial == Connectors.connectorId.
      return (
        (await repo
          .readConnectorByStationIdAndOcpp16ConnectorId(tenantId, name, p.connectorId)
          .catch(() => null)) ?? null
      );
    }
    // 2.x: payload.evseId is the OCPP evse serial, payload.connectorId the per-EVSE
    // connector serial → EVSEType {id, connectorId}.
    const byEvseType = await repo
      .readConnectorByStationIdAndOcpp201EvseType(tenantId, name, {
        id: p.evseId,
        connectorId: p.connectorId,
      })
      .catch(() => null);
    if (byEvseType) return byEvseType;
    // 1-connector-per-EVSE fleet: the 2.x evse serial equals the 1.6 connector serial.
    return (
      (await repo
        .readConnectorByStationIdAndOcpp16ConnectorId(tenantId, name, p.evseId)
        .catch(() => null)) ?? null
    );
  };
  let row = await lookup();
  const retries = Math.min(Math.max(attempts - 1, 0), ROW_RETRY_DELAYS_MS.length);
  for (let i = 0; row == null && i < retries; i++) {
    await sleep(ROW_RETRY_DELAYS_MS[i]);
    row = await lookup();
  }
  _noteRowResolution(deps.logger, row != null);
  return row;
}

async function stationNameById(stationDbId: number): Promise<string | undefined> {
  const st = await ChargingStation.findByPk(stationDbId).catch(() => null);
  return st?.ocppConnectionName ?? undefined;
}

async function stationProtocol(
  deps: GateDeps,
  tenantId: number,
  stationId: string,
): Promise<string | undefined> {
  try {
    const station = await deps.locationRepository.readChargingStationByOcppConnectionName(
      tenantId,
      stationId,
    );
    return station?.protocol ?? undefined;
  } catch {
    return undefined;
  }
}

/**
 * Read a model attribute defeating class-field shadowing: upstream beta4's
 * Transaction model declares `ocppConnectionName!: string` WITHOUT `declare`, so
 * the compiled class field shadows sequelize's prototype getter and the plain
 * property read returns undefined even though the DB column is populated.
 * `.get(key)` reads dataValues directly and is immune. (Upstream PR candidate:
 * add `declare`.)
 */
function attr<T = any>(model: any, key: string): T | undefined {
  const direct = model?.[key];
  if (direct !== undefined) return direct;
  return typeof model?.get === 'function' ? model.get(key) : undefined;
}

/**
 * Resolve a Transactions CRUD row's DB FKs down to the OCPP-facing number the
 * wallet contract expects (see ID SEMANTICS above). Prefers the connector serial
 * (matches the 1.6 path and ChargeMai's evse numbering), falls back to the 2.0.1
 * evse serial, and only then to 1 (logged — the RPC schema requires a value).
 */
async function ocppEvseIdForTx(tx: any, logger: Logger<ILogObj>): Promise<number> {
  try {
    if (tx.connectorId != null) {
      const conn = await Connector.findByPk(tx.connectorId);
      if (conn?.connectorId != null) return conn.connectorId;
    }
    if (tx.evseId != null) {
      const evse = await Evse.findByPk(tx.evseId);
      if (evse?.evseTypeId != null) return evse.evseTypeId;
    }
  } catch (err) {
    logger.warn(`ocppEvseIdForTx resolution failed for tx ${tx.transactionId}: ${err}`);
  }
  logger.warn(`tx ${tx.transactionId} has no resolvable connector/evse — defaulting evseId=1`);
  return 1;
}

async function dispatchRemoteStart(
  deps: GateDeps,
  tenantId: number,
  stationId: string,
  // Distinct on purpose — 1.6 wants the per-station connector serial, 2.x wants
  // the OCPP 2.0.1 evse serial. Passing one number for both is the M4 trap.
  ids: { ocpp16ConnectorId: number; ocpp201EvseId: number },
  idTag: string,
  idTokenType: string,
): Promise<void> {
  const protocol = await stationProtocol(deps, tenantId, stationId);
  if (!protocol) throw new Error(`unknown protocol for station ${stationId}`);
  const is16 = protocol === 'ocpp1.6';
  await deps.ocppSender.sendCall({
    ocppConnectionName: stationId,
    tenantId,
    protocol,
    action: is16 ? 'RemoteStartTransaction' : 'RequestStartTransaction',
    eventGroup: 'evdriver',
    payload: is16
      ? { idTag, connectorId: ids.ocpp16ConnectorId }
      : {
          remoteStartId: Date.now() & 0x7fffffff,
          idToken: { idToken: idTag, type: idTokenType || 'Central' },
          evseId: ids.ocpp201EvseId,
        },
  });
}

async function dispatchRemoteStop(
  deps: GateDeps,
  tenantId: number,
  stationId: string,
  transactionId: string,
): Promise<void> {
  const protocol = await stationProtocol(deps, tenantId, stationId);
  if (!protocol) throw new Error(`unknown protocol for station ${stationId}`);
  const is16 = protocol === 'ocpp1.6';
  await deps.ocppSender.sendCall({
    ocppConnectionName: stationId,
    tenantId,
    protocol,
    action: is16 ? 'RemoteStopTransaction' : 'RequestStopTransaction',
    eventGroup: 'evdriver',
    payload: is16
      ? { transactionId: parseInt(transactionId, 10) }
      : { transactionId: String(transactionId) },
  });
}

// ---- Flow 2: Preparing gate ----
// 1.6 StatusNotification Preparing / 2.x connectorStatus Occupied → ask the wallet
// whether a pre-authorized session should start; Accepted → upsert token + RemoteStart.

export class PreparingGate {
  private readonly _deps: GateDeps;
  private readonly _logger: Logger<ILogObj>;
  private readonly _rpc: AmqpRpc;
  // In-process in-flight guard: a charger can repeat StatusNotification for the
  // same plug event, and the Redis setIfNotExist may not be atomic under
  // concurrency in every cache impl.
  private readonly _inFlight = new Set<string>();

  constructor(deps: GateDeps) {
    this._deps = deps;
    this._logger = deps.logger.getSubLogger({ name: this.constructor.name });
    this._rpc = new AmqpRpc({
      channelManager: deps.channelManager,
      logger: deps.logger,
      channelId: 'preparing-rpc',
      queue: PREPARING_QUEUE,
    });
  }

  stop(): void {}

  /** StatusNotification frames only (GatesFrameSource filters by action). */
  onFrame(evt: GateFrameEvent): void {
    const status = evt.payload?.status ?? evt.payload?.connectorStatus;
    if (status !== 'Preparing' && status !== 'Occupied') return;
    setImmediate(
      () =>
        void (async () => {
          const row = await connectorRowFromFrame(this._deps, evt);
          if (!row) {
            this._logger.warn(
              `preparing gate: no connector row for ${evt.ocppConnectionName} ${JSON.stringify(evt.payload)} — skipping`,
            );
            return;
          }
          await this._fire(evt, row);
        })().catch((err) => this._logger.warn(`preparing gate frame error: ${err}`)),
    );
  }

  private async _fire(evt: GateFrameEvent, row: any): Promise<void> {
    const tenantId = evt.tenantId;
    const stationId = evt.ocppConnectionName;
    // OCPP wire numbers come from the FRAME, not the DB row: 2.x Connector rows
    // store their serial in evseTypeConnectorId/Evses.evseTypeId, leaving
    // Connectors.connectorId NULL — re-deriving from the row drops every 2.x plug.
    const p = evt.payload ?? {};
    const is16 = evt.protocol === 'ocpp1.6';
    // ChargeMai identity == the 1.6 per-station connector serial; for 2.x that maps
    // to the evse serial (1 connector per EVSE fleet-wide).
    const connectorId = (is16 ? p.connectorId : (p.evseId ?? p.connectorId)) as number | undefined;
    const ocpp201EvseId = (p.evseId ?? p.connectorId) as number | undefined;
    if (connectorId == null) {
      this._logger.warn(
        `preparing gate: no wire connector id in frame for ${stationId} ${JSON.stringify(p)} — skipping`,
      );
      return;
    }
    const dedupeKey = `prep:${tenantId}:${stationId}:${connectorId}`;
    if (this._inFlight.has(dedupeKey)) return;
    this._inFlight.add(dedupeKey);
    try {
      // Multi-replica-safe in-flight guard: only one RPC per plug event window.
      const fresh = await this._deps.cache.setIfNotExist(
        dedupeKey,
        '1',
        CACHE_NS,
        Math.ceil(RPC_TIMEOUT_MS / 1000) + 30,
      );
      if (!fresh) return;

      // Re-read guard: 2.x 'Occupied' persists through the WHOLE charge, and any
      // connector-row update re-fires this gate once the dedupe TTL lapses. A
      // connector that already has an active transaction must never trigger a
      // preparing consult (worst case: RemoteStart dispatched mid-charge). An
      // active tx with NO connector/evse linkage counts as busy too — being
      // conservative here only delays a preparing consult; being permissive
      // starts a session on an occupied connector.
      const activeTxs = await this._deps.transactionEventRepository.transaction.readAllByQuery(
        tenantId,
        { where: { stationId: row.stationId, isActive: true } },
      );
      const busy = (activeTxs ?? []).some(
        (t: any) =>
          (t.connectorId == null && t.evseId == null) ||
          (t.connectorId != null && t.connectorId === row.id) ||
          (t.evseId != null && row.evseId != null && t.evseId === row.evseId),
      );
      if (busy) {
        this._logger.debug(`preparing gate skipped — active tx on ${stationId}:${connectorId}`);
        return;
      }

      const reply = await this._rpc.call<{ status: string; idTag?: string; idTokenType?: string }>({
        tenantId,
        stationId,
        evseId: connectorId,
        connectorId,
      });
      if (reply.status !== 'Accepted') {
        this._logger.debug(`preparing gate rejected for ${stationId}:${connectorId}`);
        return;
      }
      if (!reply.idTag || !reply.idTokenType) {
        this._logger.error(
          `preparing gate Accepted without idTag/idTokenType — aborting RemoteStart`,
        );
        return;
      }
      await this._deps.authorizationRepository.ensureAccepted(
        tenantId,
        reply.idTag,
        reply.idTokenType,
      );
      // Wire numbers straight from the frame: dispatchRemoteStart picks by the
      // station's live protocol, so pass both — the 1.6 connector serial and the
      // 2.0.1 evse serial.
      await dispatchRemoteStart(
        this._deps,
        tenantId,
        stationId,
        {
          ocpp16ConnectorId: (p.connectorId ?? connectorId) as number,
          ocpp201EvseId: ocpp201EvseId ?? connectorId,
        },
        reply.idTag,
        reply.idTokenType,
      );
      this._logger.info(
        `preparing gate started session for ${stationId}:${connectorId} (idTag ${reply.idTag})`,
      );
    } catch (err) {
      this._logger.warn(`preparing gate error for ${stationId}:${connectorId}: ${err}`);
    } finally {
      this._inFlight.delete(dedupeKey);
    }
  }
}

// ---- Flow 3: SuspendedEV gate ----
// 2.x: Transactions.chargingState → SuspendedEV (transaction 'updated' events).
// 1.6: Connector status → SuspendedEV (connector events) + active-tx lookup.
// Debounce SUSPENDED_MIN_DURATION_MS (BMS oscillation), re-check state, then at
// most ONE wallet RPC per transaction (Redis dedupe; replica-safe).

export class SuspendedEvGate {
  private readonly _deps: GateDeps;
  private readonly _logger: Logger<ILogObj>;
  private readonly _rpc: AmqpRpc;
  private readonly _timers = new Map<string, NodeJS.Timeout>();

  constructor(deps: GateDeps) {
    this._deps = deps;
    this._logger = deps.logger.getSubLogger({ name: this.constructor.name });
    this._rpc = new AmqpRpc({
      channelManager: deps.channelManager,
      logger: deps.logger,
      channelId: 'suspended-ev-rpc',
      queue: SUSPENDED_QUEUE,
    });
  }

  stop(): void {
    for (const t of this._timers.values()) clearTimeout(t);
    this._timers.clear();
  }

  /** Dispatch by frame action (GatesFrameSource routes StatusNotification + TransactionEvent here). */
  onFrame(evt: GateFrameEvent): void {
    if (evt.action === 'TransactionEvent') this._onTransactionEventFrame(evt);
    else if (evt.action === 'StatusNotification' && evt.protocol === 'ocpp1.6')
      this._onStatusNotification16Frame(evt);
  }

  // 2.x: TransactionEvent frames carry (station, transactionId, chargingState)
  // directly — the timer key is wire identity (v2's Transactions unique is
  // (stationId, transactionId), so the key must include the station).
  private _onTransactionEventFrame(evt: GateFrameEvent): void {
    const p = evt.payload ?? {};
    const txId = p.transactionInfo?.transactionId;
    if (txId == null) return;
    const key = `${evt.tenantId}:tx:${evt.ocppConnectionName}:${txId}`;
    const suspended = p.eventType !== 'Ended' && p.transactionInfo?.chargingState === 'SuspendedEV';
    if (suspended) {
      this._schedule(key, async () => {
        // Post-debounce re-read: the resume/end frame may have been consumed by
        // ANOTHER replica (shared queue — its cancel can't reach our timer), so
        // trust the DB, not the frame that armed us. Every downstream value
        // comes from the re-read row.
        const sid = await stationDbIdByName(evt.tenantId, evt.ocppConnectionName);
        if (sid == null) return;
        const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(
          evt.tenantId,
          { where: { stationId: sid, transactionId: txId } },
        );
        const cur = txs?.[0];
        if (!cur?.isActive || cur.chargingState !== 'SuspendedEV') return;
        await this._fire(
          evt.tenantId,
          evt.ocppConnectionName,
          String(txId),
          await ocppEvseIdForTx(cur, this._logger),
        );
      });
    } else if (this._timers.has(key)) {
      // Resumed charging OR Ended before the debounce elapsed — cancel. The
      // post-debounce re-read is the safety net when this cancel lands on a
      // different replica than the arm.
      clearTimeout(this._timers.get(key)!);
      this._timers.delete(key);
    }
  }

  // 1.6: connector-status frames. SuspendedEV arms; any other status for the same
  // connector cancels (StopTransaction needs no explicit cancel — the post-debounce
  // re-read sees the connector/tx state moved on and bails).
  private _onStatusNotification16Frame(evt: GateFrameEvent): void {
    const p = evt.payload ?? {};
    if (p.connectorId == null) return;
    const key = `${evt.tenantId}:conn:${evt.ocppConnectionName}:${p.connectorId}`;
    if (p.status !== 'SuspendedEV') {
      if (this._timers.has(key)) {
        clearTimeout(this._timers.get(key)!);
        this._timers.delete(key);
      }
      return;
    }
    this._schedule(key, async () => {
      // Post-debounce re-read from the DB (shared queue: the resume frame may have
      // landed on another replica) — bail unless the connector is still SuspendedEV,
      // and take every field from the re-read row. Single-shot: 30s after the frame
      // the row must exist; cold-start retry would only waste queries.
      const cur = await connectorRowFromFrame(this._deps, evt, 1);
      if (!cur || cur.status !== 'SuspendedEV') return;
      // 1.6 has no chargingState on the tx — find the NEWEST active tx for this
      // station (stale never-stopped rows must not shadow the live session).
      // Prefer the tx linked to THIS connector when the linkage exists.
      const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(
        evt.tenantId,
        {
          where: { stationId: cur.stationId, isActive: true },
          order: [['createdAt', 'DESC']],
        },
      );
      const tx =
        txs?.find(
          (t: any) =>
            (t.connectorId != null && t.connectorId === cur.id) ||
            (t.evseId != null && cur.evseId != null && t.evseId === cur.evseId),
        ) ?? txs?.[0];
      if (!tx) return;
      await this._fire(evt.tenantId, evt.ocppConnectionName, tx.transactionId, cur.connectorId);
    });
  }

  private _schedule(key: string, action: () => Promise<void> | void): void {
    if (this._timers.has(key)) return;
    this._timers.set(
      key,
      setTimeout(() => {
        this._timers.delete(key);
        void (async () => {
          try {
            await action();
          } catch (err) {
            this._logger.warn(`suspendedEV gate error (${key}): ${err}`);
          }
        })();
      }, SUSPENDED_MIN_DURATION_MS),
    );
  }

  private async _fire(
    tenantId: number,
    stationId: string,
    transactionId: string,
    evseId: number,
  ): Promise<void> {
    // At most one wallet consult per transaction, across replicas.
    const fresh = await this._deps.cache.setIfNotExist(
      `susp:${tenantId}:${stationId}:${transactionId}`,
      '1',
      CACHE_NS,
      24 * 3600,
    );
    if (!fresh) return;

    const reply = await this._rpc.call<{ action: string }>({
      tenantId,
      stationId,
      evseId,
      transactionId,
      connectorId: evseId,
    });
    if (reply.action === 'Stop') {
      await dispatchRemoteStop(this._deps, tenantId, stationId, transactionId);
      this._logger.info(`suspendedEV gate stopped tx ${transactionId} on ${stationId}`);
    } else {
      this._logger.debug(`suspendedEV gate continue for tx ${transactionId}`);
    }
  }
}

// ---- Frame source: one shared durable queue on the upstream `messages` exchange ----
// The router publishes every frame (`frame.<direction>.<Action>`, topic, durable);
// we bind only the inbound actions the gates care about. Always ack: gate decisions
// are re-validated against the DB before any RPC, so a dropped frame degrades to
// "no consult this event", never to a wrong decision — and requeue loops on a
// poison frame would be worse.
const GATES_QUEUE = process.env.WALLET_GATES_QUEUE ?? 'wallet.gates';
// The exchange name + routing-key FORMAT are upstream-owned and were the real
// churn risk — anchor them to the exported constants (MESSAGES_EXCHANGE,
// FrameDirection) so a rename is a compile error here, not a silent "bound to a
// dead pattern, no frames, boot log still green" runtime failure. The action
// segments are OCPP protocol identifiers (stable across CitrineOS versions), so
// literals are fine — CallAction is a type-only export and can't be used as a value.
const STATUS_ACTION = 'StatusNotification';
const TX_ACTION = 'TransactionEvent';
const GATE_BINDINGS = [
  `frame.${FrameDirection.Inbound}.${STATUS_ACTION}`,
  `frame.${FrameDirection.Inbound}.${TX_ACTION}`,
];

export class GatesFrameSource {
  private readonly _logger: Logger<ILogObj>;
  private readonly _channelManager: any;
  private readonly _preparing?: PreparingGate;
  private readonly _suspended?: SuspendedEvGate;
  private _channel: any;
  private _consumerTag?: string;
  // Set on stop(): channel.cancel() stops NEW deliveries, but frames already
  // prefetched (≤25) are still delivered to this callback — drain them (ack, no
  // dispatch) so none arms a gate/RemoteStart after we begin shutdown.
  private _draining = false;
  // Guards ONLY the sync path (malformed JSON / a frame we can't even route). Real
  // upstream payload drift does NOT surface here — it parses fine and flows into the
  // async gates, where connectorRowFromFrame's miss-counter catches it instead.
  private _consecutiveFailures = 0;

  constructor(deps: GateDeps, gates: { preparing?: PreparingGate; suspended?: SuspendedEvGate }) {
    this._logger = deps.logger.getSubLogger({ name: this.constructor.name });
    this._channelManager = deps.channelManager;
    this._preparing = gates.preparing;
    this._suspended = gates.suspended;
  }

  async start(): Promise<void> {
    const channel = await this._channelManager.getChannel('gates-frames');
    this._channel = channel;
    // Idempotent, matches the publisher's declaration exactly (mismatched args 406).
    await channel.assertExchange(MESSAGES_EXCHANGE, 'topic', { durable: true });
    await channel.assertQueue(GATES_QUEUE, { durable: true, autoDelete: false });
    for (const key of GATE_BINDINGS) {
      await channel.bindQueue(GATES_QUEUE, MESSAGES_EXCHANGE, key);
    }
    await channel.prefetch(25);
    const { consumerTag } = await channel.consume(GATES_QUEUE, (msg: any) => {
      if (!msg) return;
      if (this._draining) {
        channel.ack(msg);
        return;
      }
      try {
        const evt: GateFrameEvent = JSON.parse(msg.content.toString());
        // Unparsed-path frames carry no payload — nothing to gate on.
        if (evt?.parsed && evt.payload != null && evt.ocppConnectionName) {
          if (evt.action === STATUS_ACTION) {
            this._preparing?.onFrame(evt);
            this._suspended?.onFrame(evt);
          } else if (evt.action === TX_ACTION) {
            this._suspended?.onFrame(evt);
          }
        }
        this._consecutiveFailures = 0;
      } catch (err) {
        // Only malformed/unroutable frames reach here (parse or sync-dispatch throw).
        this._consecutiveFailures++;
        const level = this._consecutiveFailures >= 20 ? 'error' : 'warn';
        this._logger[level](
          `gates frame parse/dispatch failed (${this._consecutiveFailures} in a row): ${err}`,
        );
      } finally {
        channel.ack(msg);
      }
    });
    this._consumerTag = consumerTag;
    this._logger.info(
      `gates consuming ${GATES_QUEUE} <- ${MESSAGES_EXCHANGE} [${GATE_BINDINGS.join(', ')}]`,
    );
  }

  async stop(): Promise<void> {
    // Drain first (in-flight prefetched frames ack without dispatching), then cancel
    // the consumer so no new frame dispatches a gate (RemoteStart/Stop) mid-drain.
    this._draining = true;
    if (this._channel && this._consumerTag) {
      try {
        await this._channel.cancel(this._consumerTag);
      } catch {
        /* channel already closing */
      }
    }
    this._consumerTag = undefined;
  }
}
