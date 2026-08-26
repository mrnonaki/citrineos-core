// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — AMQP RPC client to the wallet app (flow 1).
//
// Wire contract preserved verbatim from the 1.9.1 fork (wallet side unchanged):
//   queue `citrineos.rabbitmq.auth` (env RABBITMQ_AUTH_QUEUE), durable, default exchange
//   request  {tenantId, stationId, idToken, idTokenType}
//   reply    {status, cacheExpiryDateTime?}  — status = 2.0.1 AuthorizationStatusEnum
//   replyTo  = server-named EXCLUSIVE queue (assertQueue('', {exclusive:true})),
//              consumed noAck; correlationId = uuid, echoed verbatim by the wallet
//   props    contentType application/json, persistent:false
//   timeout  RABBITMQ_TIMEOUT_MS (default 10000; deployments set 30000)
//
// Channel comes from upstream ChannelManager (auto-recreated on reconnect) — we
// re-assert the reply queue whenever the channel instance changes.

import { randomUUID } from 'node:crypto';
import type { ILogObj, Logger } from 'tslog';

const REQUEST_QUEUE = process.env.RABBITMQ_AUTH_QUEUE ?? 'citrineos.rabbitmq.auth';
const RPC_TIMEOUT_MS = Number(process.env.RABBITMQ_TIMEOUT_MS ?? 10000);
const CHANNEL_ID = 'auth-rpc';

// Same detection as the 1.9.1 fork: MAC forms → MacAddress, else ISO14443.
const MAC_RE = /^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$|^[0-9A-Fa-f]{12}$/;

export function detectTokenType(idToken: string, provided?: string | null): string {
  if (provided) return provided;
  return MAC_RE.test(idToken) ? 'MacAddress' : 'ISO14443';
}

export interface WalletAuthRequest {
  tenantId: number;
  stationId: string;
  idToken: string;
  idTokenType: string;
}

export interface WalletAuthDecision {
  status: string; // AuthorizationStatusEnum value
  cacheExpiryDateTime?: string | null;
}

interface WalletRpcClientCradle {
  channelManager: any;
  logger: Logger<ILogObj>;
}

export class WalletRpcClient {
  private readonly _channelManager: any;
  private readonly _logger: Logger<ILogObj>;
  private _setup: { channel: any; replyQueue: string } | null = null;
  private readonly _pending = new Map<
    string,
    { resolve: (d: WalletAuthDecision) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }
  >();

  constructor({ channelManager, logger }: WalletRpcClientCradle) {
    this._channelManager = channelManager;
    this._logger = logger.getSubLogger({ name: this.constructor.name });
  }

  async authorizeToken(request: WalletAuthRequest): Promise<WalletAuthDecision> {
    const { channel, replyQueue } = await this._ensureSetup();
    const correlationId = randomUUID();

    return new Promise<WalletAuthDecision>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(correlationId);
        reject(new Error(`wallet auth RPC timeout after ${RPC_TIMEOUT_MS}ms`));
      }, RPC_TIMEOUT_MS);

      this._pending.set(correlationId, {
        resolve: (d) => {
          clearTimeout(timer);
          resolve(d);
        },
        reject: (e) => {
          clearTimeout(timer);
          reject(e);
        },
        timer,
      });

      channel.sendToQueue(REQUEST_QUEUE, Buffer.from(JSON.stringify(request)), {
        correlationId,
        replyTo: replyQueue,
        contentType: 'application/json',
        persistent: false,
      });
    });
  }

  private async _ensureSetup(): Promise<{ channel: any; replyQueue: string }> {
    const channel = await this._channelManager.getChannel(CHANNEL_ID);
    if (this._setup && this._setup.channel === channel) {
      return this._setup;
    }
    // New or recreated channel: fail pending requests fast, re-assert topology.
    for (const [id, p] of this._pending) {
      p.reject(new Error('wallet auth RPC channel recreated'));
      this._pending.delete(id);
    }
    await channel.assertQueue(REQUEST_QUEUE, { durable: true });
    const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true });
    await channel.consume(
      replyQueue,
      (msg: any) => {
        if (!msg) return;
        const pending = this._pending.get(msg.properties.correlationId);
        if (!pending) return;
        this._pending.delete(msg.properties.correlationId);
        try {
          pending.resolve(JSON.parse(msg.content.toString()));
        } catch (err) {
          pending.reject(new Error(`wallet auth RPC reply parse error: ${err}`));
        }
      },
      { noAck: true },
    );
    this._setup = { channel, replyQueue };
    this._logger.info(`wallet auth RPC ready (queue=${REQUEST_QUEUE}, reply=${replyQueue})`);
    return this._setup;
  }
}
