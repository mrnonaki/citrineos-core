// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — flows 2 & 3, attached via repository CRUD events
// (NOT handler subclasses): StatusNotificationResponse is `{}` in both protocols,
// so gates can only react out-of-band — events give that with zero handler edits.
// CAVEAT: CRUD events fire PRE-COMMIT (inside the sequelize tx) — every handler
// defers via setImmediate, and every RPC-firing path re-reads current state first
// (active-tx guard in PreparingGate; tx/connector re-read after the SuspendedEV
// debounce) because with >1 replica the cancelling event may be processed by a
// DIFFERENT replica than the one that armed the timer.
//
// ID SEMANTICS (trap): CRUD rows carry DB PKs, not OCPP numbers.
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

async function stationNameById(stationDbId: number): Promise<string | undefined> {
  const st = await ChargingStation.findByPk(stationDbId).catch(() => null);
  return st?.ocppConnectionName ?? undefined;
}

async function stationProtocol(deps: GateDeps, tenantId: number, stationId: string): Promise<string | undefined> {
  try {
    const station = await deps.locationRepository.readChargingStationByOcppConnectionName(tenantId, stationId);
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
    payload: is16 ? { transactionId: parseInt(transactionId, 10) } : { transactionId: String(transactionId) },
  });
}

// ---- Flow 2: Preparing gate ----
// 1.6 StatusNotification Preparing / 2.x connectorStatus Occupied → ask the wallet
// whether a pre-authorized session should start; Accepted → upsert token + RemoteStart.

export class PreparingGate {
  private readonly _deps: GateDeps;
  private readonly _logger: Logger<ILogObj>;
  private readonly _rpc: AmqpRpc;
  private readonly _listener = (rows: any[]) => this._onConnectorRows(rows);
  // In-process in-flight guard: one StatusNotification can update the connector
  // row more than once (two 'updated' emits back-to-back) and the Redis
  // setIfNotExist may not be atomic under concurrency in every cache impl.
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

  start(): void {
    const connectorRepo = this._deps.locationRepository.connector;
    connectorRepo.on('created', this._listener);
    connectorRepo.on('updated', this._listener);
    this._logger.info('preparing gate listening on connector CRUD events');
  }

  stop(): void {
    const connectorRepo = this._deps.locationRepository.connector;
    connectorRepo.off('created', this._listener);
    connectorRepo.off('updated', this._listener);
  }

  private _onConnectorRows(rows: any[]): void {
    for (const row of rows ?? []) {
      if (row?.status !== 'Preparing' && row?.status !== 'Occupied') continue;
      setImmediate(() => void this._fire(row));
    }
  }

  private async _fire(eventRow: any): Promise<void> {
    // CRUD 'updated' rows can be PARTIAL (only touched columns + PK) — re-read by
    // PK whenever a needed field is missing, and use the re-read from there on.
    let row = eventRow;
    if (row.stationId == null || row.connectorId == null) {
      row = (await Connector.findByPk(eventRow.id).catch(() => null)) ?? eventRow;
      if (row.status !== 'Preparing' && row.status !== 'Occupied') return; // state moved on
    }
    const tenantId = row.tenantId;
    const stationId = row.stationId != null ? await stationNameById(row.stationId) : undefined;
    const connectorId = row.connectorId;
    if (!stationId || connectorId == null) {
      this._logger.warn(`preparing gate: connector ${eventRow.id} unresolvable — skipping`);
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
        this._logger.error(`preparing gate Accepted without idTag/idTokenType — aborting RemoteStart`);
        return;
      }
      await this._deps.authorizationRepository.ensureAccepted(tenantId, reply.idTag, reply.idTokenType);
      // Connector row: connectorId = 1.6 serial; evseId = Evses DB FK → resolve
      // the 2.0.1 serial via Evses.evseTypeId (fall back to the connector serial —
      // correct while the fleet is 1 connector per EVSE).
      let ocpp201EvseId = connectorId;
      if (row.evseId != null) {
        const evse = await Evse.findByPk(row.evseId).catch(() => null);
        if (evse?.evseTypeId != null) ocpp201EvseId = evse.evseTypeId;
      }
      await dispatchRemoteStart(
        this._deps,
        tenantId,
        stationId,
        { ocpp16ConnectorId: connectorId, ocpp201EvseId },
        reply.idTag,
        reply.idTokenType,
      );
      this._logger.info(`preparing gate started session for ${stationId}:${connectorId} (idTag ${reply.idTag})`);
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
  private readonly _txListener = (rows: any[]) => this._onTransactionRows(rows);
  private readonly _connListener = (rows: any[]) => this._onConnectorRows(rows);

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

  start(): void {
    this._deps.transactionEventRepository.transaction.on('updated', this._txListener);
    const connectorRepo = this._deps.locationRepository.connector;
    connectorRepo.on('updated', this._connListener);
    this._logger.info('suspendedEV gate listening on transaction/connector CRUD events');
  }

  stop(): void {
    this._deps.transactionEventRepository.transaction.off('updated', this._txListener);
    this._deps.locationRepository.connector.off('updated', this._connListener);
    for (const t of this._timers.values()) clearTimeout(t);
    this._timers.clear();
  }

  private _onTransactionRows(rows: any[]): void {
    for (const row of rows ?? []) {
      if (!row) continue;
      // Key must NOT include fields that may be absent on PARTIAL event rows
      // (arm on a full row + cancel on a partial one would miss the timer), and
      // must be PK-based: OCPP transactionId repeats across stations (unique is
      // (stationId, transactionId) in v2).
      const key = `${row.tenantId}:tx:${row.id}`;
      if (row.isActive && row.chargingState === 'SuspendedEV') {
        this._schedule(key, async () => {
          // Post-debounce re-read: the resume/end event may have been processed
          // by ANOTHER replica (its cancel can't reach our timer), so trust the
          // DB, not the event that armed us. Re-read by PK — CRUD 'updated' rows
          // can be PARTIAL (only touched columns), but the PK is always present,
          // and v2's Transactions unique is (stationId, transactionId), so the
          // OCPP transactionId ALONE can match another station's tx (observed on
          // kind: 1.6 tx id 7 on two stations). Every downstream value comes
          // from the re-read row, not the event row.
          const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(
            row.tenantId,
            { where: { id: row.id } },
          );
          const cur = txs?.[0];
          if (!cur?.isActive || cur.chargingState !== 'SuspendedEV') return;
          const stationId = cur.stationId != null ? await stationNameById(cur.stationId) : undefined;
          if (!stationId) {
            this._logger.warn(`suspendedEV: tx ${row.transactionId} has no resolvable station — skipping`);
            return;
          }
          await this._fire(
            row.tenantId,
            stationId,
            row.transactionId,
            await ocppEvseIdForTx(cur, this._logger),
          );
        });
      } else if (this._timers.has(key)) {
        // Resumed charging OR ended (isActive=false) before the debounce elapsed
        // — cancel. NOTE: ended rows MUST reach this branch; guarding the loop
        // with `!row.isActive → continue` made it unreachable (fixed).
        clearTimeout(this._timers.get(key)!);
        this._timers.delete(key);
      }
    }
  }

  private _onConnectorRows(rows: any[]): void {
    for (const row of rows ?? []) {
      if (row?.status !== 'SuspendedEV') continue;
      // PK-based key: partial event rows always carry the PK.
      const key = `${row.tenantId}:conn:${row.id}`;
      this._schedule(key, async () => {
        // Post-debounce re-read (multi-replica: the resume event may have landed
        // on another replica; event rows can be PARTIAL) — bail unless the
        // connector is still SuspendedEV, and take every field from the re-read.
        const cur = await Connector.findByPk(row.id).catch(() => null);
        if (!cur || cur.status !== 'SuspendedEV') return;
        // 1.6 has no chargingState on the tx — find the NEWEST active tx for this
        // station (stale never-stopped rows must not shadow the live session).
        // Prefer the tx linked to THIS connector when the linkage exists.
        const stationName = cur.stationId != null ? await stationNameById(cur.stationId) : undefined;
        if (!stationName) return;
        const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(row.tenantId, {
          where: { stationId: cur.stationId, isActive: true },
          order: [['createdAt', 'DESC']],
        });
        const tx =
          txs?.find(
            (t: any) =>
              (t.connectorId != null && t.connectorId === cur.id) ||
              (t.evseId != null && cur.evseId != null && t.evseId === cur.evseId),
          ) ?? txs?.[0];
        if (!tx) return;
        await this._fire(row.tenantId, stationName, tx.transactionId, cur.connectorId);
      });
    }
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
