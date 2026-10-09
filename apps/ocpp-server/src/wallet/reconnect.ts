// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — re-subscribe wallet AMQP consumers after a broker reconnect.
//
// RabbitMQChannelManager recreates its channels when the connection manager emits
// 'connected' after a reconnect, but NOT the consumers on them. Upstream's own
// receivers (RabbitMqReceiver, MessagesEventConsumer) re-subscribe on that event;
// ours subscribed once at start, so after a broker restart `wallet.gates`,
// `citrineos.rabbitmq.remotestart` and `.remotestop` sat at 0 consumers until the
// modules pod was restarted (seen on prod r3 and reproduced on next 862ce929a).

import type { ILogObj, Logger } from 'tslog';

// Backoff for a re-subscribe that fails (e.g. the new connection drops again while
// we re-declare). After the last attempt we stay down until the NEXT reconnect, loudly.
const RETRY_DELAYS_MS = [1_000, 2_000, 5_000, 10_000, 30_000];

/**
 * Calls `resubscribe` every time the broker connection comes back. Returns a
 * disposer that removes the listener and cancels any pending retry (call it from
 * the consumer's stop()). `resubscribe` must be idempotent: the hook may fire while
 * the consumer's channel is still alive, so it should skip when already consuming.
 */
export function onBrokerReconnect(
  channelManager: any,
  logger: Logger<ILogObj>,
  label: string,
  resubscribe: () => Promise<void>,
): () => void {
  const cm =
    typeof channelManager?.getConnectionManager === 'function'
      ? channelManager.getConnectionManager()
      : undefined;
  if (!cm || typeof cm.on !== 'function') {
    logger.warn(
      `${label}: channelManager exposes no connection manager — will NOT re-subscribe after a broker reconnect`,
    );
    return () => {};
  }

  let disposed = false;
  let running = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const attempt = async (n: number): Promise<void> => {
    if (disposed) return;
    running = true;
    try {
      await resubscribe();
      logger.info(`${label}: re-subscribed after broker reconnect`);
    } catch (err) {
      if (disposed) return;
      if (n < RETRY_DELAYS_MS.length) {
        const delay = RETRY_DELAYS_MS[n];
        logger.warn(
          `${label}: re-subscribe failed (attempt ${n + 1}), retrying in ${delay} ms: ${err}`,
        );
        timer = setTimeout(() => {
          timer = undefined;
          void attempt(n + 1);
        }, delay);
        return; // stay "running" until the retry chain ends
      }
      logger.error(
        `${label}: re-subscribe failed ${n + 1} times — consumer stays DOWN until the next broker reconnect: ${err}`,
      );
    }
    running = false;
  };

  const handler = () => {
    if (disposed || running) return; // one re-subscribe chain at a time
    void attempt(0);
  };
  cm.on('connected', handler);

  return () => {
    disposed = true;
    if (timer) clearTimeout(timer);
    timer = undefined;
    const off = cm.off ?? cm.removeListener;
    if (typeof off === 'function') off.call(cm, 'connected', handler);
  };
}
