// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — flows 4 & 5: wallet → CSMS push consumers.
//
// Wire contract (1.9.1, wallet side unchanged):
//   remotestart: {stationId, idTag, evseId, connectorId?, chargingProfile?, request?}
//   remotestop:  {stationId, transactionId, request?}
//   Both: durable queue, prefetch 8, ALWAYS ack (no nack/requeue/DLQ); reply only
//   when msg.properties.replyTo is set (correlationId echoed, IMessageConfirmation
//   or {success:false, payload:"<error>"}).
//
// Tenant resolution: WALLET_TENANT_ID (default 1). The 1.9.1 fork resolved tenant
// by scanning stations across tenants; our deploys are single-tenant. TODO(multi-
// tenant): station→tenant lookup before Phase 6 if that ever changes.

import type { ILogObj, Logger } from 'tslog';
import type { WalletAuthorizationRepository } from './WalletAuthorizationRepository.js';

const REMOTESTART_QUEUE = process.env.RABBITMQ_REMOTESTART_QUEUE ?? 'citrineos.rabbitmq.remotestart';
const REMOTESTOP_QUEUE = process.env.RABBITMQ_REMOTESTOP_QUEUE ?? 'citrineos.rabbitmq.remotestop';
const TENANT_ID = Number(process.env.WALLET_TENANT_ID ?? 1);

export interface ConsumerDeps {
  channelManager: any;
  logger: Logger<ILogObj>;
  locationRepository: any;
  transactionEventRepository: any;
  authorizationRepository: WalletAuthorizationRepository;
  ocppSender: any;
}

abstract class WalletConsumer {
  protected readonly _deps: ConsumerDeps;
  protected readonly _logger: Logger<ILogObj>;
  private readonly _channelId: string;
  private readonly _queue: string;

  constructor(deps: ConsumerDeps, channelId: string, queue: string) {
    this._deps = deps;
    this._logger = deps.logger.getSubLogger({ name: `${this.constructor.name}` });
    this._channelId = channelId;
    this._queue = queue;
  }

  async start(): Promise<void> {
    const channel = await this._deps.channelManager.getChannel(this._channelId);
    await channel.assertQueue(this._queue, { durable: true });
    await channel.prefetch(8);
    await channel.consume(this._queue, (msg: any) => {
      if (!msg) return;
      void (async () => {
        let result: any;
        try {
          const payload = JSON.parse(msg.content.toString());
          result = await this.handle(payload);
        } catch (err) {
          result = { success: false, payload: String(err) };
          this._logger.warn(`${this._queue} message failed: ${err}`);
        } finally {
          // Contract: always ack — failures are reported via replyTo, never requeued.
          channel.ack(msg);
        }
        if (msg.properties?.replyTo) {
          try {
            channel.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(result ?? {})), {
              correlationId: msg.properties.correlationId,
              contentType: 'application/json',
              persistent: false,
            });
          } catch (err) {
            this._logger.warn(`${this._queue} reply failed: ${err}`);
          }
        }
      })();
    });
    this._logger.info(`consuming ${this._queue}`);
  }

  async stop(): Promise<void> {
    if (typeof this._deps.channelManager.closeChannel !== 'function') {
      // Upstream ChannelManager without closeChannel: the consumer keeps its
      // channel until process exit. Loud, so a "stopped" consumer that still
      // consumes is explainable from logs.
      this._logger.warn(`stop(): channelManager has no closeChannel — ${this._queue} consumer not detached`);
      return;
    }
    await this._deps.channelManager.closeChannel(this._channelId);
  }

  protected async stationProtocol(stationId: string): Promise<string> {
    const station = await this._deps.locationRepository.readChargingStationByStationId(
      TENANT_ID,
      stationId,
    );
    if (!station?.protocol) throw new Error(`Unknown station or NULL protocol: ${stationId}`);
    return station.protocol;
  }

  protected abstract handle(payload: any): Promise<any>;
}

export class RemoteStartConsumer extends WalletConsumer {
  constructor(deps: ConsumerDeps) {
    super(deps, 'remotestart-consumer', REMOTESTART_QUEUE);
  }

  protected async handle(payload: any): Promise<any> {
    const { stationId, idTag, chargingProfile, request } = payload ?? {};
    const evseId = payload?.evseId ?? payload?.connectorId;
    if (!stationId || (!request && (!idTag || !evseId))) {
      return { success: false, payload: 'Invalid payload: need stationId + (idTag & evseId) or request' };
    }
    const protocol = await this.stationProtocol(stationId);
    const is16 = protocol === 'ocpp1.6';

    if (idTag) {
      // Pre-auth upsert so the charger-side Authorize/StartTransaction hits Accepted.
      // Failures are logged but do not block dispatch (matches 1.9.1 behavior).
      try {
        await this._deps.authorizationRepository.ensureAccepted(TENANT_ID, idTag, 'Central');
      } catch (err) {
        this._logger.warn(`ensureAccepted failed for ${idTag}: ${err}`);
      }
    }

    const ocppPayload =
      request ??
      (is16
        ? { idTag, connectorId: evseId, ...(chargingProfile ? { chargingProfile } : {}) }
        : {
            remoteStartId: Date.now() & 0x7fffffff,
            idToken: { idToken: idTag, type: 'Central' },
            evseId,
            ...(chargingProfile ? { chargingProfile } : {}),
          });

    return await this._deps.ocppSender.sendCall({
      ocppConnectionName: stationId,
      tenantId: TENANT_ID,
      protocol,
      action: is16 ? 'RemoteStartTransaction' : 'RequestStartTransaction',
      eventGroup: 'evdriver',
      payload: ocppPayload,
    });
  }
}

export class RemoteStopConsumer extends WalletConsumer {
  constructor(deps: ConsumerDeps) {
    super(deps, 'remotestop-consumer', REMOTESTOP_QUEUE);
  }

  protected async handle(payload: any): Promise<any> {
    const { stationId, request } = payload ?? {};
    const transactionId = payload?.transactionId != null ? String(payload.transactionId) : undefined;
    if (!stationId || (!request && !transactionId)) {
      return { success: false, payload: 'Invalid payload: need stationId + transactionId or request' };
    }
    const protocol = await this.stationProtocol(stationId);
    const is16 = protocol === 'ocpp1.6';

    if (transactionId) {
      // Plan-C idempotency: unknown tx → error; already-stopped → success, no dispatch.
      const txs = await this._deps.transactionEventRepository.transaction.readAllByQuery(TENANT_ID, {
        where: { ocppConnectionName: stationId, transactionId },
      });
      const tx = txs?.[0];
      if (!tx) return { success: false, payload: `Unknown transaction: ${transactionId}` };
      if (!tx.isActive) {
        return {
          success: true,
          payload: { alreadyStopped: true, endedAt: tx.endTime ?? null, stoppedReason: tx.stoppedReason ?? null },
        };
      }
    }

    return await this._deps.ocppSender.sendCall({
      ocppConnectionName: stationId,
      tenantId: TENANT_ID,
      protocol,
      action: is16 ? 'RemoteStopTransaction' : 'RequestStopTransaction',
      eventGroup: 'evdriver',
      payload: request ?? (is16 ? { transactionId: parseInt(transactionId!, 10) } : { transactionId: transactionId! }),
    });
  }
}
