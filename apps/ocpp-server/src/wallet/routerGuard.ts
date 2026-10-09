// SPDX-License-Identifier: Apache-2.0
// ChargeMai fork — boot guards for the lean router entrypoint (router-main.ts). Kept
// apart from registerWalletServices.ts so the router never loads the wallet auth layer.

import { RabbitMqRouterReceiver } from '@citrineos/ocpp';
import type { SystemConfig } from '@citrineos/types';
import type { AwilixContainer } from 'awilix';

// Multi-replica ROUTER guard (dist/wallet/router-main.js only).
// Upstream (#1162) registers routerHandler as a RabbitMqRouterReceiver: ONE queue per
// router instance (`rabbit_queue_router_<instanceIdentifier>`) + a headers binding per
// connected charger. That per-instance queue is only correct if instanceIdentifier is
// UNIQUE per pod — a shared id makes two pods consume one queue and round-robins ALL
// traffic — and an unset id silently falls back to `router-${Date.now()}` (a fresh
// queue name on every restart). So we HARD-FAIL if it is unset or blank. The deploy
// sets it from the pod name (valueFrom metadata.name →
// CITRINEOS_MESSAGEBROKER_AMQP_INSTANCEIDENTIFIER), which is unique + stable per pod.
//
// Guard only: nothing is registered. (The r3 shim re-registered routerHandler with
// routerMode:true; upstream removed that option and made RabbitMqReceiver abstract.)
// Call from registerAdditionalServices(), i.e. before the router receiver exists.
export function assertRouterInstanceIdentifier(container: AwilixContainer): void {
  const config = container.resolve<SystemConfig>('config');
  const instanceId = config?.messageBroker?.amqp?.instanceIdentifier;
  if (!instanceId || !instanceId.trim()) {
    throw new Error(
      'wallet: the router requires messageBroker.amqp.instanceIdentifier ' +
        '(env CITRINEOS_MESSAGEBROKER_AMQP_INSTANCEIDENTIFIER), unique per pod — ' +
        'refusing to start the router without a unique instance queue.',
    );
  }
}

// Boot-time tripwire for the router: the guard above is only meaningful while the
// router really runs the per-instance-queue receiver. Upstream renaming the token or
// swapping the class would otherwise go unnoticed. Call once the container is wired.
export function assertRouterReceiver(container: AwilixContainer): void {
  const handler: unknown = container.resolve('routerHandler');
  if (!(handler instanceof RabbitMqRouterReceiver)) {
    const actual = (handler as { constructor?: { name?: string } })?.constructor?.name;
    throw new Error(
      `wallet: routerHandler is ${actual ?? typeof handler}, expected RabbitMqRouterReceiver ` +
        '— upstream router wiring changed?',
    );
  }
}
