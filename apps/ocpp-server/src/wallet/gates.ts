// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — flows 2 & 3, attached via repository CRUD events
// (NOT handler subclasses): StatusNotificationResponse is `{}` in both protocols,
// so gates can only react out-of-band — events give that with zero handler edits.
// CAVEAT: CRUD events fire PRE-COMMIT (inside the sequelize tx) — every handler
// defers via setImmediate and re-reads state before acting.
//
// Wire contract (1.9.1, wallet side unchanged):
//   preparing: req {tenantId, stationId, evseId, connectorId?}
//              rep {status:'Accepted'|'Rejected', idTag, idTokenType}
//   suspended: req {tenantId, stationId, evseId, transactionId, connectorId?}
//              rep {action:'Stop'|'Continue'}

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

async function stationProtocol(deps: GateDeps, tenantId: number, stationId: string): Promise<string | undefined> {
  try {
    const station = await deps.locationRepository.readChargingStationByStationId(tenantId, stationId);
    return station?.protocol ?? undefined;
  } catch {
    return undefined;
  }
}

async function dispatchRemoteStart(
  deps: GateDeps,
  tenantId: number,
  stationId: string,
  evseId: number,
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
      ? { idTag, connectorId: evseId }
      : {
          remoteStartId: Date.now() & 0x7fffffff,
          idToken: { idToken: idTag, type: idTokenType || 'Central' },
          evseId,
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

  private async _fire(row: any): Promise<void> {
    const tenantId = row.tenantId;
    const stationId = row.ocppConnectionName;
    const connectorId = row.connectorId;
    const dedupeKey = `prep:${tenantId}:${stationId}:${connectorId}`;
    try {
      // Multi-replica-safe in-flight guard: only one RPC per plug event window.
      const fresh = await this._deps.cache.setIfNotExist(
        dedupeKey,
        '1',
        CACHE_NS,
        Math.ceil(RPC_TIMEOUT_MS / 1000) + 30,
      );
      if (!fresh) return;

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
      await dispatchRemoteStart(this._deps, tenantId, stationId, connectorId, reply.idTag, reply.idTokenType);
      this._logger.info(`preparing gate started session for ${stationId}:${connectorId} (idTag ${reply.idTag})`);
    } catch (err) {
      this._logger.warn(`preparing gate error for ${stationId}:${connectorId}: ${err}`);
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
      if (!row?.isActive) continue;
      const key = `${row.tenantId}:${row.ocppConnectionName}:${row.transactionId}`;
      if (row.chargingState === 'SuspendedEV') {
        this._schedule(key, () =>
          this._fire(row.tenantId, row.ocppConnectionName, row.transactionId, row.evseDatabaseId ?? 1),
        );
      } else if (this._timers.has(key)) {
        // Charging resumed before the debounce elapsed — cancel.
        clearTimeout(this._timers.get(key)!);
        this._timers.delete(key);
      }
    }
  }

  private _onConnectorRows(rows: any[]): void {
    for (const row of rows ?? []) {
      if (row?.status !== 'SuspendedEV') continue;
      const key = `${row.tenantId}:${row.ocppConnectionName}:conn${row.connectorId}`;
      this._schedule(key, async () => {
        // 1.6 has no chargingState on the tx — find the active tx for this station.
        const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(row.tenantId, {
          where: { ocppConnectionName: row.ocppConnectionName, isActive: true },
        });
        const tx = txs?.[0];
        if (!tx) return;
        await this._fire(row.tenantId, row.ocppConnectionName, tx.transactionId, row.connectorId);
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
