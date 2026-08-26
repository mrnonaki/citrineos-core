// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — AMQP RPC to the wallet app (shared mechanics for the
// auth / preparing / suspended gates).
//
// Wire contract preserved verbatim from the 1.9.1 fork (wallet side unchanged):
//   request queue: named, durable, default exchange
//   replyTo      = server-named EXCLUSIVE queue (assertQueue('', {exclusive:true})),
//                  consumed noAck; correlationId = uuid, echoed verbatim by the wallet
//   props        contentType application/json, persistent:false
//   timeout      RABBITMQ_TIMEOUT_MS (default 10000; deployments set 30000)
//
// Channels come from upstream ChannelManager (auto-recreated on reconnect) — we
// re-assert the reply queue whenever the channel instance changes.

import { randomUUID } from 'node:crypto';
import type { ILogObj, Logger } from 'tslog';

export const RPC_TIMEOUT_MS = Number(process.env.RABBITMQ_TIMEOUT_MS ?? 10000);

// Same detection as the 1.9.1 fork: MAC forms → MacAddress, else ISO14443.
const MAC_RE = /^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$|^[0-9A-Fa-f]{12}$/;

export function detectTokenType(idToken: string, provided?: string | null): string {
  if (provided) return provided;
  return MAC_RE.test(idToken) ? 'MacAddress' : 'ISO14443';
}

export interface AmqpRpcOptions {
  channelManager: any;
  logger: Logger<ILogObj>;
  channelId: string;
  queue: string;
  timeoutMs?: number;
}

export class AmqpRpc {
  protected readonly _channelManager: any;
  protected readonly _logger: Logger<ILogObj>;
  private readonly _channelId: string;
  private readonly _queue: string;
  private readonly _timeoutMs: number;
  private _setup: { channel: any; replyQueue: string } | null = null;
  private readonly _pending = new Map<
    string,
    { resolve: (d: any) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }
  >();

  constructor({ channelManager, logger, channelId, queue, timeoutMs }: AmqpRpcOptions) {
    this._channelManager = channelManager;
    this._logger = logger.getSubLogger({ name: `${this.constructor.name}:${channelId}` });
    this._channelId = channelId;
    this._queue = queue;
    this._timeoutMs = timeoutMs ?? RPC_TIMEOUT_MS;
  }

  async call<TReply = any>(payload: object): Promise<TReply> {
    const { channel, replyQueue } = await this._ensureSetup();
    const correlationId = randomUUID();

    return new Promise<TReply>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(correlationId);
        reject(new Error(`wallet RPC (${this._queue}) timeout after ${this._timeoutMs}ms`));
      }, this._timeoutMs);

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

      channel.sendToQueue(this._queue, Buffer.from(JSON.stringify(payload)), {
        correlationId,
        replyTo: replyQueue,
        contentType: 'application/json',
        persistent: false,
      });
    });
  }

  private async _ensureSetup(): Promise<{ channel: any; replyQueue: string }> {
    const channel = await this._channelManager.getChannel(this._channelId);
    if (this._setup && this._setup.channel === channel) {
      return this._setup;
    }
    // New or recreated channel: fail pending requests fast, re-assert topology.
    for (const [id, p] of this._pending) {
      p.reject(new Error(`wallet RPC channel ${this._channelId} recreated`));
      this._pending.delete(id);
    }
    await channel.assertQueue(this._queue, { durable: true });
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
          pending.reject(new Error(`wallet RPC reply parse error: ${err}`));
        }
      },
      { noAck: true },
    );
    this._setup = { channel, replyQueue };
    this._logger.info(`wallet RPC ready (queue=${this._queue}, reply=${replyQueue})`);
    return this._setup;
  }
}

// ---- Flow 1: auth ----

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

export class WalletRpcClient extends AmqpRpc {
  constructor({ channelManager, logger }: WalletRpcClientCradle) {
    super({
      channelManager,
      logger,
      channelId: 'auth-rpc',
      queue: process.env.RABBITMQ_AUTH_QUEUE ?? 'citrineos.rabbitmq.auth',
    });
  }

  async authorizeToken(request: WalletAuthRequest): Promise<WalletAuthDecision> {
    return this.call<WalletAuthDecision>(request);
  }
}
